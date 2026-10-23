# Bounded fault-runner hardening, 2026-10-23

The full final framework suite passed (`main-full-go.log`, exit 0), followed
by successful CLI compilation. Commands were `go test ./... -count=1
-timeout 10m` and `go build -o validation-runs/hardening-20261023/failforge-final
./cmd/failforge`. The previous agent's failing reproduction and successful
focused/full attempts are retained separately, not relabelled.

Implementation: immutable single-use matrix outputs, copied binary hash,
before/after target source fingerprints, explicit repetitions/seed range,
bounded runtime and sampled storage, per-file limits, confirmed fault and
successful-operation coverage, non-green unavailable/partial evidence, and
exact owned-descendant cleanup. Fault injection/recovery failures now fail a
run independently of minimum successful injection counts. Background recovery
work drains before audit/history close. Seed zero remains a valid explicit seed.

The first 24-attempt matrix was deliberately interrupted after Main committed
MiniDB while it was active. Seeds 42 and 43 passed; seed 44 correctly reported
PROVENANCE_CHANGED. The mixed-revision matrix remains INTERRUPTED in
`../hardening-matrix-20261023/summary.json`; it is not release proof. Main sent
SIGINT only through a pinned process handle to the exact owned supervisor,
allowing bounded cleanup. A fresh matrix must run only after all target/tool
commits are frozen. No first attempt or failure is replaced by that rerun.

CI now requires the complete framework suite, race detector, existing covered
first-party campaigns, and eight bounded independent-repetition runner attempts.
Actual final pushed-head CI and the final frozen-revision local matrix remain
separate gates. Long-running production failure schedules and exhaustive
partitions are not established by these bounded profiles.
