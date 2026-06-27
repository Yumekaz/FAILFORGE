# FailForge

**A local deterministic failure testing lab for controlled distributed systems.**

FailForge is a local failure testing framework designed for distributed key-value stores, coordination engines, and consensus protocols. By routing all client-to-node and node-to-node traffic through a reverse proxy and using process group signaling, FailForge can inject seeded or scripted faults (such as network partitions, clock skews, crashes, packet corruption, and disk failures) deterministically, detect correctness violations, and use delta debugging to isolate the minimal sequence of events that triggers a bug.

---

## Why FailForge Exists

Distributed systems are notoriously difficult to test. Transient bugs like split-brain, lost acknowledged writes, and read-after-write violations often require a precise interleaving of network delays and process crashes to manifest.

FailForge brings deterministic chaos testing to local development. It is designed to bridge the gap between simple unit tests and heavyweight, production chaos frameworks (like Jepsen). It allows you to:
1. **Coordinate Local Nodes**: Spin up multi-node process clusters locally.
2. **Control the Network**: Intercept all node communications via an HTTP reverse proxy.
3. **Inject Seeded Chaos**: Generate deterministic fault schedules from a single seed.
4. **Isolate Bugs via Delta Debugging**: Automatically minimize fault schedules, operation count, and runtimes using delta debugging (DDMin) to isolate root causes.
5. **Visualize Timelines**: Analyze failures using colorized terminal prints, Mermaid sequence diagrams, and interactive, self-contained HTML dashboards.

### What FailForge is Not
* **Not a Production Chaos Tool**: FailForge is designed for local developer-first simulation testing, not for running chaos in live Kubernetes or production cloud environments.
* **Not an Arbitrary Packet Interceptor**: It relies on HTTP reverse proxying and cooperative node headers, avoiding complex, low-level TCP raw socket manipulation.
* **Not a Formal Verifier**: It runs state-based invariant checking over execution histories rather than mathematically proving correctness.

---

## Supported Faults & Invariants

FailForge comes with a polymorphic fault engine supporting **15 distinct network, process, filesystem, and logical faults**:

| Category | Type | Parameter Description |
|---|---|---|
| **Process** | `kill_node` | Abruptly terminates a node using SIGKILL. |
| | `restart_node` | Terminates and safely restarts a node. |
| | `cpu_pause` | Simulates GC pauses/stalls by suspending nodes using SIGSTOP/SIGCONT. |
| | `slow_disk` | Simulates disk stalls by cycling nodes through micro-suspensions. |
| **Network** | `partition` | Creates symmetric network partitions between groups of nodes. |
| | `asymmetric_partition` | Sets up a one-way partition (A cannot send to B, but B can send to A). |
| | `delay_messages` | Injects message latency between selected endpoints. |
| | `drop_messages` | Drops messages between selected endpoints. |
| | `duplicate_messages` | Delivers clone requests of messages. |
| | `corrupt_messages` | Randomly flips bits in HTTP request bodies at a custom rate. |
| | `heal` | Clears all active partitions, drops, delays, duplications, and corruptions. |
| **Filesystem** | `disk_write_loss` | Crashes a node and truncates files modified within a window to 0 bytes. |
| | `partial_persistence` | Crashes a node and truncates its newest file to 50%–90% of its size. |
| | `stale_snapshot_restart` | Reverts a node's data directory to an older automatically saved snapshot. |
| **Logical** | `clock_skew` | Injects virtual clock drift on messages via headers. |

### Supported Correctness Checkers
* **`read_after_acknowledged_write`**: Verifies register consistency. If a write completes at time $T$, no subsequent read operation can return a value older than that write.

---

## Architecture

```
                      +-------------------+
                      |   Client Workload |
                      +---------+---------+
                                | (HTTP / Proxy Routing)
                                v
                      +---------+---------+
                      |  FailForge Proxy  |<----+ Intercepts, Delays,
                      +---------+---------+     | Drops, & Duplicates
                                |               | Messages
                                v               |
              +-----------------+-----------------+
              |                 |                 |
              v                 v                 v
        +-----------+     +-----------+     +-----------+
        |  Node-1   |     |  Node-2   |     |  Node-3   |
        +-----+-----+     +-----+-----+     +-----+-----+
              |                 |                 |
              +---------------->+---------------->+ (Node-to-Node Reverse Proxy)
```

1. **Runner**: Coordinates the lifecycle of the simulation, orchestrating the workload, scheduler, proxy, and checkers.
2. **NodeManager**: Declares and spawns local processes under separate process groups to prevent subprocess leakage.
3. **Proxy**: Acts as the reverse proxy. All client-to-node and node-to-node HTTP requests are intercepted to check rules (e.g. partition drops, corruption rates).
4. **Scheduler**: Translates a pseudorandom seed (or scripted YAML timeline) into a deterministic queue of fault injections.

---

## Quickstart

### Prerequisites
* Go 1.21+
* python3 (for the default toy-kv example node)

### 1. Build the Binary
```bash
go build -o bin/failforge ./cmd/failforge
```

### 2. Run a Seeded Chaos Campaign
Run a single simulation run using the default config:
```bash
./bin/failforge run failforge.yml --seed 42
```

### 3. Minimize a Failed Run
If a run fails due to an invariant violation, minimize it to find the simplest reproduction:
```bash
./bin/failforge minimize runs/42
```
This automatically prunes unnecessary client operations, fault schedules, and runtimes.

### 4. View the Interactive Timeline
Generate and open the interactive dashboard for a run:
```bash
./bin/failforge timeline runs/42 --html
```
This generates `runs/42/timeline.html`. Open it in any web browser to view the chronological logs, CSS stats cards, and the Mermaid sequence diagram.

---

## Writing an Adapter

To test your own distributed key-value store, database, or coordination engine, configure it using a YAML file:

```yaml
name: my-distributed-db-test
seed: 12345
time:
  duration_ms: 15000
  tick_ms: 10
system:
  type: process_cluster
  nodes:
    count: 3
    # FailForge starts nodes using command placeholders:
    command: "./bin/my-db --id {node_id} --port {port} --proxy-url {proxy_url} --data-dir {data_dir}"
    ports:
      start: 8000
    data_dir: "/tmp/my-db/{node_id}"
network:
  mode: proxy
  proxy_port: 9000
workload:
  type: kv-register
  clients: 2
  duration_ms: 10000
  keys: [x]
  operations:
    put: { weight: 5 }
    get: { weight: 5 }
checkers:
  - name: read_after_acknowledged_write
output:
  dir: "runs/{seed}"
```

### Node Cooperative Guidelines
To get the most out of advanced network faults:
1. **Clock Skew**: Nodes should extract the `X-FailForge-Clock-Offset` header from incoming HTTP requests and apply it to their virtual clock to test time-drift bugs.
2. **Reverse Proxying**: Make sure node-to-node requests are sent via the proxy URL (passed dynamically using `{proxy_url}`) using the `X-FailForge-From` and `X-FailForge-To` headers to route traffic correctly.

---

## Limitations

* **Single Host**: Nodes must be runnable as local subprocesses on the same machine.
* **HTTP/HTTP2 Only**: Network fault interception is implemented via reverse proxying HTTP requests. Raw TCP socket interception is not supported.
* **Cooperative Clock Skew**: Process clock skew relies on the application reading the custom HTTP header rather than operating system level time namespace virtualization.
