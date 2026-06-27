package runner

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

// GenerateTerminalTimeline outputs a colorized chronological timeline string of events.
func GenerateTerminalTimeline(runID string, st *store.Store) (string, error) {
	events, err := st.GetEvents(runID)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("\033[1;36m=== CHRONOLOGICAL EVENT TIMELINE ===\033[0m\n")

	for _, e := range events {
		timestamp := fmt.Sprintf("\033[34m[%4dms]\033[0m", e.TimeMs)
		category := ""
		colorPrefix := ""
		colorSuffix := "\033[0m"

		switch e.Category {
		case "Run":
			if e.Type == "Violation" {
				category = "⚠️  [VIOLATION]"
				colorPrefix = "\033[1;31m" // Bold Red
			} else {
				category = "⚙️  [RUN]"
				colorPrefix = "\033[36m" // Cyan
			}
		case "Node":
			category = "🖥️  [NODE]"
			colorPrefix = "\033[33m" // Yellow
		case "Fault":
			category = "💥 [FAULT]"
			colorPrefix = "\033[1;35m" // Bold Magenta
		case "Operation":
			category = "📥 [OP]"
			if e.Type == "OperationInvoked" {
				colorPrefix = "\033[32m" // Green
			} else if e.Type == "OperationCompleted" {
				colorPrefix = "\033[1;32m" // Bold Green
			} else {
				colorPrefix = "\033[2m" // Dim
			}
		default:
			category = fmt.Sprintf("[%s]", e.Category)
			colorPrefix = "\033[0m"
		}

		desc := formatEventText(e)
		sb.WriteString(fmt.Sprintf("%s %s%s %s%s\n", timestamp, colorPrefix, category, desc, colorSuffix))
	}

	return sb.String(), nil
}

// GenerateMermaidSequence creates a Mermaid sequence diagram string from events and messages.
func GenerateMermaidSequence(runID string, st *store.Store) (string, error) {
	events, err := st.GetEvents(runID)
	if err != nil {
		return "", err
	}

	messages, err := st.GetMessages(runID)
	if err != nil {
		return "", err
	}

	// 1. Discover unique participants (nodes and clients)
	nodesMap := make(map[string]bool)
	clientsMap := make(map[string]bool)

	for _, e := range events {
		var payload map[string]interface{}
		_ = json.Unmarshal([]byte(e.PayloadJSON), &payload)

		if e.Category == "Node" {
			if nodeID, ok := payload["node_id"].(string); ok && nodeID != "" {
				nodesMap[nodeID] = true
			}
		}
		if e.Category == "Operation" {
			if clientID, ok := payload["client_id"].(string); ok && clientID != "" {
				clientsMap[clientID] = true
			}
			if target, ok := payload["target"].(string); ok && target != "" {
				nodesMap[target] = true
			}
		}
	}

	for _, m := range messages {
		if m.FromNode != "" {
			nodesMap[m.FromNode] = true
		}
		if m.ToNode != "" {
			nodesMap[m.ToNode] = true
		}
	}

	var clients []string
	for c := range clientsMap {
		clients = append(clients, c)
	}
	sort.Strings(clients)

	var nodes []string
	for n := range nodesMap {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	var sb strings.Builder
	sb.WriteString("sequenceDiagram\n")
	sb.WriteString("    autonumber\n")

	// Declare participants
	for _, c := range clients {
		sb.WriteString(fmt.Sprintf("    actor %s\n", c))
	}
	for _, n := range nodes {
		sb.WriteString(fmt.Sprintf("    participant %s\n", n))
	}

	// Filter message logic: show all client ops, all faults, all dropped/delayed network msgs,
	// and at most 30 happy-path messages to prevent sequence diagram pollution.
	happyMsgsCount := 0
	const maxHappyMsgs = 30

	// Interleave events and messages chronologically
	type ChronoItem struct {
		TimeMs   int64
		Category string
		Type     string
		Text     string
	}

	var items []ChronoItem

	for _, e := range events {
		var payload map[string]interface{}
		_ = json.Unmarshal([]byte(e.PayloadJSON), &payload)

		switch e.Category {
		case "Operation":
			clientID, _ := payload["client_id"].(string)
			target, _ := payload["target"].(string)
			op, _ := payload["op"].(string)
			key, _ := payload["key"].(string)
			val, _ := payload["value"].(string)
			if val == "" {
				if numVal, ok := payload["value"].(float64); ok {
					val = fmt.Sprintf("%g", numVal)
				}
			}

			if e.Type == "OperationInvoked" {
				if target != "" {
					opText := fmt.Sprintf("%s(%s)", op, key)
					if val != "" {
						opText = fmt.Sprintf("%s(%s=%s)", op, key, val)
					}
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    %s->>+%s: %s", clientID, target, opText),
					})
				} else {
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    Note over %s: Invoke %s key '%s'", clientID, op, key),
					})
				}
			} else if e.Type == "OperationCompleted" {
				status, _ := payload["status"].(string)
				result, _ := payload["result"].(string)
				opText := status
				if result != "" && result != "null" {
					opText = fmt.Sprintf("%s: %s", status, result)
				}
				if target != "" {
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    %s-->>-%s: %s", target, clientID, opText),
					})
				} else {
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    Note over %s: Operation Completed: %s", clientID, opText),
					})
				}
			}

		case "Fault":
			nodeID, _ := payload["node"].(string)
			if nodeID == "" {
				nodeID, _ = payload["node_id"].(string)
			}
			faultType := e.Type

			if nodeID != "" {
				items = append(items, ChronoItem{
					TimeMs: e.TimeMs,
					Text:   fmt.Sprintf("    Note over %s: FAULT: %s Injected", nodeID, faultType),
				})
			} else {
				// Global fault like partition or heal
				if faultType == "partition" {
					var groups [][]string
					if gData, ok := payload["groups"]; ok {
						if bytes, err := json.Marshal(gData); err == nil {
							_ = json.Unmarshal(bytes, &groups)
						}
					}
					var groupStrs []string
					for _, g := range groups {
						groupStrs = append(groupStrs, "["+strings.Join(g, ",")+"]")
					}
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    Note over %s: FAULT: Network Partition Injected %s", nodes[0], strings.Join(groupStrs, " / ")),
					})
				} else {
					items = append(items, ChronoItem{
						TimeMs: e.TimeMs,
						Text:   fmt.Sprintf("    Note over %s: FAULT: Network healed", nodes[0]),
					})
				}
			}

		case "Run":
			if e.Type == "Violation" {
				checker, _ := payload["checker_name"].(string)
				desc, _ := payload["description"].(string)
				items = append(items, ChronoItem{
					TimeMs: e.TimeMs,
					Text:   fmt.Sprintf("    Note over %s: ⚠️ VIOLATION: %s - %s", nodes[0], checker, desc),
				})
			}
		}
	}

	for _, m := range messages {
		// Only display sent node-to-node messages
		if m.FromNode == "" || m.ToNode == "" {
			continue
		}

		msgType := m.MessageType
		if msgType == "" {
			msgType = "msg"
		}

		if m.Status == "dropped" {
			items = append(items, ChronoItem{
				TimeMs: m.SendMs,
				Text:   fmt.Sprintf("    %s-x%s: %s (DROPPED)", m.FromNode, m.ToNode, msgType),
			})
		} else if m.Status == "delayed" {
			items = append(items, ChronoItem{
				TimeMs: m.SendMs,
				Text:   fmt.Sprintf("    %s-->>%s: %s (DELAYED)", m.FromNode, m.ToNode, msgType),
			})
		} else if m.Status == "delivered" {
			happyMsgsCount++
			if happyMsgsCount <= maxHappyMsgs {
				items = append(items, ChronoItem{
					TimeMs: m.SendMs,
					Text:   fmt.Sprintf("    %s->>%s: %s", m.FromNode, m.ToNode, msgType),
				})
			}
		}
	}

	// Sort chronological items
	sort.Slice(items, func(i, j int) bool {
		return items[i].TimeMs < items[j].TimeMs
	})

	for _, it := range items {
		sb.WriteString(it.Text + "\n")
	}

	return sb.String(), nil
}

// GenerateHTMLTimeline creates a self-contained visual dashboard report.html inside runDir.
func GenerateHTMLTimeline(runID string, st *store.Store, runDir string) error {
	events, err := st.GetEvents(runID)
	if err != nil {
		return err
	}


	ops, err := st.GetOperations(runID)
	if err != nil {
		return err
	}

	violations, err := st.GetViolations(runID)
	if err != nil {
		return err
	}

	mermaidScript, _ := GenerateMermaidSequence(runID, st)

	// Build raw events JSON array
	type JSONEvent struct {
		TimeMs   int64  `json:"time_ms"`
		Category string `json:"category"`
		Type     string `json:"type"`
		Text     string `json:"text"`
		Payload  string `json:"payload"`
	}

	var jsonEvents []JSONEvent
	for _, e := range events {
		jsonEvents = append(jsonEvents, JSONEvent{
			TimeMs:   e.TimeMs,
			Category: e.Category,
			Type:     e.Type,
			Text:     formatEventText(e),
			Payload:  e.PayloadJSON,
		})
	}
	eventsBytes, _ := json.MarshalIndent(jsonEvents, "", "  ")

	// Calculate stats
	totalOps := len(ops)
	successOps := 0
	for _, op := range ops {
		if op.Status == "ok" {
			successOps++
		}
	}
	failOps := totalOps - successOps

	// Load SQLite Run Info
	var seed int64
	var status string
	db, errDb := sql.Open("sqlite", filepath.Join(runDir, "history.sqlite"))
	if errDb == nil {
		defer db.Close()
		_ = db.QueryRow("SELECT seed, status FROM runs ORDER BY started_at DESC LIMIT 1").Scan(&seed, &status)
	}

	htmlContent := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FailForge Visual Timeline: Run %s</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0f172a;
            --panel-bg: #1e293b;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent-color: #6366f1;
            --success-color: #10b981;
            --fail-color: #ef4444;
            --warning-color: #f59e0b;
            --border-color: #334155;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Outfit', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-primary);
            line-height: 1.5;
            padding: 2rem;
        }

        header {
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        h1 {
            font-size: 2rem;
            font-weight: 700;
            background: linear-gradient(135deg, #818cf8, #c084fc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .run-status {
            font-size: 0.9rem;
            font-weight: 600;
            padding: 0.4rem 1rem;
            border-radius: 9999px;
            text-transform: uppercase;
        }

        .status-passed {
            background-color: rgba(16, 185, 129, 0.2);
            color: var(--success-color);
            border: 1px solid var(--success-color);
        }

        .status-failed {
            background-color: rgba(239, 68, 68, 0.2);
            color: var(--fail-color);
            border: 1px solid var(--fail-color);
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .stat-card {
            background-color: var(--panel-bg);
            border: 1px solid var(--border-color);
            padding: 1.5rem;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            transition: transform 0.2s;
        }

        .stat-card:hover {
            transform: translateY(-2px);
        }

        .stat-label {
            font-size: 0.85rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }

        .stat-value {
            font-size: 1.8rem;
            font-weight: 700;
        }

        .accent {
            color: var(--accent-color);
        }

        .main-layout {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
        }

        @media (max-width: 1024px) {
            .main-layout {
                grid-template-columns: 1fr;
            }
        }

        .column-title {
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .filter-panel {
            background-color: var(--panel-bg);
            border: 1px solid var(--border-color);
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1.5rem;
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            align-items: center;
        }

        .filter-btn {
            background-color: var(--bg-color);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            padding: 0.4rem 0.8rem;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.85rem;
            font-weight: 600;
            transition: all 0.2s;
        }

        .filter-btn.active {
            background-color: var(--accent-color);
            color: white;
            border-color: var(--accent-color);
        }

        .timeline-container {
            position: relative;
            padding-left: 2rem;
            border-left: 2px solid var(--border-color);
            max-height: 800px;
            overflow-y: auto;
            padding-right: 1rem;
        }

        .timeline-item {
            position: relative;
            background-color: var(--panel-bg);
            border: 1px solid var(--border-color);
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            cursor: pointer;
            transition: all 0.2s;
        }

        .timeline-item:hover {
            border-color: var(--accent-color);
        }

        .timeline-dot {
            position: absolute;
            left: -2.4rem;
            top: 1.2rem;
            width: 12px;
            height: 12px;
            border-radius: 50%%;
            background-color: var(--text-secondary);
            border: 2px solid var(--bg-color);
        }

        .category-Run .timeline-dot { background-color: var(--accent-color); }
        .category-Node .timeline-dot { background-color: var(--warning-color); }
        .category-Fault .timeline-dot { background-color: #d946ef; }
        .category-Operation .timeline-dot { background-color: var(--success-color); }
        .type-Violation .timeline-dot { background-color: var(--fail-color); }

        .item-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.5rem;
            font-size: 0.85rem;
        }

        .item-time {
            font-family: 'JetBrains Mono', monospace;
            font-weight: 700;
            color: var(--accent-color);
        }

        .item-badge {
            font-size: 0.75rem;
            text-transform: uppercase;
            font-weight: 600;
            padding: 0.1rem 0.5rem;
            border-radius: 4px;
            background-color: rgba(255, 255, 255, 0.1);
        }

        .badge-violation {
            background-color: rgba(239, 68, 68, 0.2);
            color: var(--fail-color);
        }

        .item-text {
            font-size: 0.95rem;
            font-weight: 500;
        }

        .item-payload {
            display: none;
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid var(--border-color);
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            background-color: rgba(0, 0, 0, 0.2);
            padding: 0.5rem;
            border-radius: 4px;
            overflow-x: auto;
            color: #38bdf8;
        }

        .sequence-column {
            background-color: var(--panel-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            max-height: 866px;
            overflow: auto;
            display: flex;
            flex-direction: column;
        }

        .mermaid {
            background-color: #020617;
            padding: 1rem;
            border-radius: 8px;
            min-height: 500px;
        }
    </style>
</head>
<body>

    <header>
        <div>
            <h1>FailForge Visual Timeline</h1>
            <p style="color: var(--text-secondary); margin-top: 0.25rem;">Run ID: <strong style="color: white;">%s</strong> &bull; Seed: <strong style="color: white;">%d</strong></p>
        </div>
        <span class="run-status %s">%s</span>
    </header>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total Duration</div>
            <div class="stat-value">%dms</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Client Operations</div>
            <div class="stat-value accent">%d</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Successful / Failed</div>
            <div class="stat-value" style="color: var(--success-color);">%d <span style="color: var(--text-secondary); font-size: 1.2rem;">/</span> <span style="color: var(--fail-color);">%d</span></div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Invariant Violations</div>
            <div class="stat-value" style="color: var(--fail-color);">%d</div>
        </div>
    </div>

    <div class="main-layout">
        <div>
            <div class="column-title">⏳ Event Timeline</div>
            <div class="filter-panel">
                <span style="font-size: 0.85rem; font-weight: 600; color: var(--text-secondary);">Filters:</span>
                <button class="filter-btn active" onclick="toggleFilter('All', this)">All</button>
                <button class="filter-btn" onclick="toggleFilter('Node', this)">Node Lifecycle</button>
                <button class="filter-btn" onclick="toggleFilter('Operation', this)">Client Ops</button>
                <button class="filter-btn" onclick="toggleFilter('Fault', this)">Fault Injections</button>
                <button class="filter-btn" onclick="toggleFilter('Violation', this)">Violations</button>
            </div>

            <div class="timeline-container" id="timeline-container">
                <!-- Javascript builds items here -->
            </div>
        </div>

        <div class="sequence-column">
            <div class="column-title">📊 Mermaid Sequence Diagram</div>
            <div class="mermaid">
%s
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({
            startOnLoad: true,
            theme: 'dark',
            sequence: {
                showSequenceNumbers: true,
                actorMargin: 50,
                width: 150,
                height: 65
            }
        });

        const rawEvents = %s;
        let currentFilter = 'All';

        function renderTimeline() {
            const container = document.getElementById('timeline-container');
            container.innerHTML = '';

            const filtered = rawEvents.filter(e => {
                if (currentFilter === 'All') return true;
                if (currentFilter === 'Violation') return e.type === 'Violation';
                return e.category === currentFilter;
            });

            if (filtered.length === 0) {
                container.innerHTML = '<div style="color: var(--text-secondary); text-align: center; padding: 2rem;">No events match the active filter.</div>';
                return;
            }

            filtered.forEach(e => {
                const item = document.createElement('div');
                item.className = 'timeline-item category-' + e.category + ' type-' + e.type;
                item.onclick = () => togglePayload(item);

                const badgeClass = e.type === 'Violation' ? 'badge-violation' : '';
                const badgeText = e.type === 'Violation' ? 'VIOLATION' : e.category;

                let formattedPayload = '';
                try {
                    formattedPayload = JSON.stringify(JSON.parse(e.payload), null, 2);
                } catch(err) {
                    formattedPayload = e.payload;
                }

                item.innerHTML = 
                    '<div class="timeline-dot"></div>' +
                    '<div class="item-header">' +
                    '    <span class="item-time">' + e.time_ms + 'ms</span>' +
                    '    <span class="item-badge ' + badgeClass + '">' + badgeText + '</span>' +
                    '</div>' +
                    '<div class="item-text">' + escapeHtml(e.text) + '</div>' +
                    '<pre class="item-payload">' + escapeHtml(formattedPayload) + '</pre>';
                container.appendChild(item);
            });
        }

        function togglePayload(item) {
            const payload = item.querySelector('.item-payload');
            if (payload.style.display === 'block') {
                payload.style.display = 'none';
            } else {
                payload.style.display = 'block';
            }
        }

        function toggleFilter(filter, btn) {
            currentFilter = filter;
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            renderTimeline();
        }

        function escapeHtml(str) {
            return str
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        // Initial render
        renderTimeline();
    </script>
</body>
</html>`,
		runID,
		runID, seed,
		statusBadgeClass(status), status,
		getRunDurationMs(events),
		totalOps,
		successOps, failOps,
		len(violations),
		html.EscapeString(mermaidScript),
		string(eventsBytes),
	)

	htmlPath := filepath.Join(runDir, "timeline.html")
	return os.WriteFile(htmlPath, []byte(htmlContent), 0644)
}

func statusBadgeClass(status string) string {
	switch status {
	case "PASSED":
		return "status-passed"
	case "FAILED":
		return "status-failed"
	default:
		return "status-failed"
	}
}

func getRunDurationMs(events []*model.Event) int64 {
	if len(events) == 0 {
		return 0
	}
	return events[len(events)-1].TimeMs
}

func formatEventText(e *model.Event) string {
	var payload map[string]interface{}
	_ = json.Unmarshal([]byte(e.PayloadJSON), &payload)

	switch e.Category {
	case "Run":
		if e.Type == "Violation" {
			desc, _ := payload["description"].(string)
			return fmt.Sprintf("Invariant Violation (%s): %s", payload["checker_name"], desc)
		}
		return fmt.Sprintf("Run Status: %s", e.Type)
	case "Node":
		nodeID, _ := payload["node_id"].(string)
		if nodeID == "" {
			nodeID = e.Type
		}
		details := ""
		if pid, ok := payload["pid"]; ok {
			details = fmt.Sprintf(" (PID: %v, Port: %v)", pid, payload["port"])
		}
		return fmt.Sprintf("Node %s: %s%s", nodeID, e.Type, details)
	case "Fault":
		nodeID, _ := payload["node"].(string)
		nodeStr := ""
		if nodeID != "" {
			nodeStr = fmt.Sprintf(" on node %s", nodeID)
		}
		return fmt.Sprintf("Fault Injected: %s%s - %s", e.Type, nodeStr, e.PayloadJSON)
	case "Operation":
		opID, _ := payload["op_id"].(string)
		clientID, _ := payload["client_id"].(string)
		op, _ := payload["op"].(string)
		key, _ := payload["key"].(string)
		if e.Type == "OperationInvoked" {
			target, _ := payload["target"].(string)
			return fmt.Sprintf("Operation Invoked by %s: %s key '%s' targeting %s (ID: %s)", clientID, strings.ToUpper(op), key, target, opID)
		} else if e.Type == "OperationCompleted" {
			status, _ := payload["status"].(string)
			latency, _ := payload["latency_ms"].(float64)
			return fmt.Sprintf("Operation Completed by %s: %s -> %s (latency: %gms, ID: %s)", clientID, strings.ToUpper(op), status, latency, opID)
		}
		return fmt.Sprintf("Operation %s: %s", e.Type, e.PayloadJSON)
	default:
		return fmt.Sprintf("%s: %s", e.Type, e.PayloadJSON)
	}
}
