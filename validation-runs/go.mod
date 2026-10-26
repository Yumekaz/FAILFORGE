// Retained source snapshots are evidence, not additional framework packages.
// This module boundary prevents `go test ./...` in the real project from
// compiling incomplete copied packages. No production tests live here.
module failforge-validation-evidence

go 1.26.4
