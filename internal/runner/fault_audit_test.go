package runner

import (
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
)

func TestCleanupRejectsFaultFailuresDespiteConfirmedCoverage(t *testing.T) {
	for _, failure := range []string{"FaultInjectionFailed", "FaultRecoveryFailed"} {
		t.Run(failure, func(t *testing.T) {
			cfg := &config.Config{Faults: config.FaultsConfig{MinimumSuccessfulInjections: 1}}
			r, err := NewRunner(cfg, nil, filepath.Join(t.TempDir(), "run"))
			if err != nil {
				t.Fatal(err)
			}
			defer r.store.Close()
			rec := &model.Run{ID: r.runID, StartedAt: time.Now(), Status: "RUNNING"}
			if err := r.store.CreateRun(rec); err != nil {
				t.Fatal(err)
			}
			r.logEvent(1, "Fault", "FaultInjected", `{"type":"cpu_pause"}`)
			r.logEvent(2, "Fault", failure, `{"fault":"cpu_pause","error":"recovery failed"}`)
			r.cleanup(rec, "PASSED")
			if rec.Status != "FAILED" {
				t.Fatalf("fault failure hidden by successful coverage: status=%s", rec.Status)
			}
		})
	}
}

func TestExplicitZeroSeedIsReproducible(t *testing.T) {
	seed := int64(0)
	r, err := NewRunner(&config.Config{}, &seed, filepath.Join(t.TempDir(), "run"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.store.Close()
	defer r.jsonlFile.Close()
	if r.seed != 0 {
		t.Fatalf("explicit seed zero changed to random seed %d", r.seed)
	}
}
