package checkers

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"failforge/internal/model"
)

func checkLockHistory(t *testing.T, ops []*model.Operation) []model.Violation {
	t.Helper()
	st, runID, cleanup := setupTestStore(t)
	defer cleanup()
	for _, op := range ops {
		copy := *op
		copy.RunID = runID
		if err := st.CreateOperation(&copy); err != nil {
			t.Fatal(err)
		}
	}
	violations, err := (&LockExclusivityChecker{}).Check(runID, st)
	if err != nil {
		t.Fatal(err)
	}
	return violations
}

func seed43LockHistory(t *testing.T) []*model.Operation {
	t.Helper()
	data, err := os.ReadFile("testdata/coordination-seed-43-lock-b.json")
	if err != nil {
		t.Fatal(err)
	}
	var ops []*model.Operation
	if err := json.Unmarshal(data, &ops); err != nil {
		t.Fatal(err)
	}
	return ops
}

func TestLockExclusivitySeed43TTLHistory(t *testing.T) {
	// Exact SQLite rows, including failed release, repeated token 88, token 125,
	// and the final operation at 38764ms. Reacquiring token 88 did not renew it.
	if got := checkLockHistory(t, seed43LockHistory(t)); len(got) != 0 {
		t.Fatalf("expired token 88 must not overlap token 125: %+v", got)
	}
}

func TestLockExclusivitySeed43RejectsEarlySuccessor(t *testing.T) {
	ops := seed43LockHistory(t)
	for _, op := range ops {
		if op.OpID == "op-client-3-150" {
			// Keep the larger fencing token: it must NOT excuse an early grant.
			op.StartMs, op.EndMs = 37000, 37010
		}
	}
	if got := checkLockHistory(t, ops); len(got) != 1 {
		t.Fatalf("a second grant before token 88 expires must fail: %+v", got)
	}
}

// Mandatory replay of every original operation, including failed and non-lock
// operations. A compressed fixture avoids a local-path dependency or CI skip.
func TestLockExclusivityRecordedCampaigns(t *testing.T) {
	encoded, err := os.ReadFile("testdata/coordination-campaigns-43-44.json.gz.b64")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := gzip.NewReader(base64.NewDecoder(base64.StdEncoding, bytes.NewReader(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	data, err := io.ReadAll(io.LimitReader(archive, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	if actual := fmt.Sprintf("%x", sha256.Sum256(data)); actual != "1e1f79424f189d726ad4d2eb95e1595f2335e3ae8683e0befd9219599ecaa221" {
		t.Fatal("recorded operation corpus integrity mismatch")
	}
	var histories []struct {
		Seed       int                `json:"seed"`
		Operations []*model.Operation `json:"operations"`
	}
	if err := json.Unmarshal(data, &histories); err != nil {
		t.Fatal(err)
	}
	if len(histories) != 2 {
		t.Fatal("expected both complete recorded histories")
	}
	for _, history := range histories {
		t.Run(fmt.Sprintf("seed-%d", history.Seed), func(t *testing.T) {
			want := map[int]int{43: 438, 44: 365}[history.Seed]
			if want == 0 || len(history.Operations) != want {
				t.Fatalf("incomplete history: %d operations, want %d", len(history.Operations), want)
			}
			if got := checkLockHistory(t, history.Operations); len(got) != 0 {
				t.Fatalf("%d operations: %+v", want, got)
			}
			t.Logf("replayed all %d operations without a local-path prerequisite", want)
		})
	}
}

func lockHistoryOp(id, client, kind string, start, end int64) *model.Operation {
	return &model.Operation{OpID: id, ClientID: client, Operation: kind,
		InputJSON: `{"key":"l1"}`, StartMs: start, EndMs: end, Status: "ok"}
}

func ttlHistoryOp(id, client string, token int, start, end int64) *model.Operation {
	op := lockHistoryOp(id, client, "LOCK", start, end)
	op.InputJSON = fmt.Sprintf(`{"key":"l1","session_id":%q}`, client)
	body, _ := json.Marshal(map[string]interface{}{
		"path": "l1", "session_id": client, "lease_token": token,
		"acquired_at": 1000.0, "expires_at": 1010.0, "lease_ttl_seconds": 10.0,
	})
	out, _ := json.Marshal(map[string]interface{}{"status_code": 200, "body": string(body)})
	op.OutputJSON = string(out)
	return op
}

func TestLockExclusivityBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []*model.Operation
		want int
	}{
		{"reentry must not erase an earlier overlap", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("b", "b", "LOCK", 11, 15),
			lockHistoryOp("bu", "b", "UNLOCK", 20, 25),
			lockHistoryOp("ar", "a", "LOCK", 30, 35),
		}, 1},
		{"release response can arrive after successor grant", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("au", "a", "UNLOCK", 20, 40),
			lockHistoryOp("b", "b", "LOCK", 21, 25),
		}, 0},
		{"grant before release invocation is still invalid", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("au", "a", "UNLOCK", 20, 40),
			lockHistoryOp("b", "b", "LOCK", 11, 15),
		}, 1},
		{"terminal successful grant is not an empty interval", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("b", "b", "LOCK", 11, 15),
		}, 1},
		{"same-millisecond grants at history end", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("b", "b", "LOCK", 0, 10),
		}, 1},
		{"no metadata must not invent a TTL", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			lockHistoryOp("b", "b", "LOCK", 20000, 20010),
		}, 1},
		{"before earliest TTL expiry", []*model.Operation{
			ttlHistoryOp("a", "a", 1, 10, 20),
			ttlHistoryOp("b", "b", 2, 10000, 10009),
		}, 1},
		{"at earliest TTL expiry", []*model.Operation{
			ttlHistoryOp("a", "a", 1, 10, 20),
			ttlHistoryOp("b", "b", 2, 10000, 10010),
		}, 0},
		{"acquire latency does not extend TTL", []*model.Operation{
			ttlHistoryOp("a", "a", 1, 0, 9000),
			ttlHistoryOp("b", "b", 2, 11000, 11010),
		}, 0},
		{"failed unlock does not end ownership", []*model.Operation{
			lockHistoryOp("a", "a", "LOCK", 0, 10),
			{OpID: "au", ClientID: "a", Operation: "UNLOCK", InputJSON: `{"key":"l1"}`, StartMs: 11, EndMs: 12, Status: "fail"},
			lockHistoryOp("b", "b", "LOCK", 13, 15),
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkLockHistory(t, tc.ops); len(got) != tc.want {
				t.Fatalf("got %d violations, want %d: %+v", len(got), tc.want, got)
			}
		})
	}
}

func TestLockExclusivitySessionIdentity(t *testing.T) {
	t.Run("different sessions on the same client are distinct owners", func(t *testing.T) {
		a := ttlHistoryOp("a", "session-a", 1, 0, 10)
		b := ttlHistoryOp("b", "session-b", 2, 20, 30)
		a.ClientID, b.ClientID = "same-client", "same-client"
		if got := checkLockHistory(t, []*model.Operation{a, b}); len(got) != 1 {
			t.Fatalf("overlapping sessions must fail, even on one client: %+v", got)
		}
	})
	t.Run("shared session reentry and release refer to one grant", func(t *testing.T) {
		a := ttlHistoryOp("a", "shared", 1, 0, 10)
		b := ttlHistoryOp("b", "shared", 1, 20, 30)
		a.ClientID, b.ClientID = "client-a", "client-b"
		release := lockHistoryOp("u", "client-b", "UNLOCK", 40, 50)
		release.InputJSON = `{"key":"l1","session_id":"shared"}`
		c := ttlHistoryOp("c", "other-session", 2, 60, 70)
		if got := checkLockHistory(t, []*model.Operation{a, b, release, c}); len(got) != 0 {
			t.Fatalf("same-session reentry is not another holder: %+v", got)
		}
	})
}

func TestLockExclusivityMalformedLeaseEvidenceFailsClosed(t *testing.T) {
	for _, output := range []string{
		`not-json`,
		`{"status_code":200,"body":"not-json"}`,
		`{"status_code":200,"body":"{}"}`,
		`{"status_code":200,"body":"{\"path\":\"l1\",\"session_id\":\"s\",\"lease_token\":1,\"lease_ttl_seconds\":10}"}`,
	} {
		t.Run(output, func(t *testing.T) {
			st, runID, cleanup := setupTestStore(t)
			defer cleanup()
			op := ttlHistoryOp("a", "s", 1, 0, 10)
			op.RunID, op.OutputJSON = runID, output
			if err := st.CreateOperation(op); err != nil {
				t.Fatal(err)
			}
			if _, err := (&LockExclusivityChecker{}).Check(runID, st); err == nil {
				t.Fatal("unreadable lease evidence must not silently pass")
			}
		})
	}
}
