# Recorded lock lifecycle corpus

`coordination-campaigns-43-44.json.gz.b64` is deterministic gzip (mtime zero),
base64-wrapped JSON of all 438 and 365 operations from the preserved seed-43/44
campaigns. Decoded JSON SHA-256:
`1e1f79424f189d726ad4d2eb95e1595f2335e3ae8683e0befd9219599ecaa221`.

The projection keeps all nine operation fields and every operation/status,
ordered by start/end/ID. It contains synthetic test values and expired local
test-session UUIDs, not host credentials. Original SQLite files, WAL hashes,
node logs and causal analysis remain under the retained validation record.

The mandatory test verifies the complete corpus and runs it through the actual
checker; no external local file or environment flag is needed. The smaller
`coordination-seed-43-lock-b.json` is the exact 39-row implicated-key fixture used
both for the false-positive regression and a deliberately early successor that
must still fail exclusivity.
