package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"failforge/internal/checkers"
	"failforge/internal/config"
	"failforge/internal/faults"
	"failforge/internal/model"
	"failforge/internal/node"
	"failforge/internal/proxy"
	"failforge/internal/store"
	"failforge/internal/workload"
)

type Runner struct {
	cfg       *config.Config
	runID     string
	seed      int64
	outputDir string
	store     *store.Store
	manager   *node.NodeManager
	proxy     *proxy.Proxy
	jsonlFile *os.File
	scheduler *faults.Scheduler
}

func NewRunner(cfg *config.Config, overrideSeed *int64, overrideOutputDir string) (*Runner, error) {
	// 1. Determine run ID and seed
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano())

	seed := cfg.Seed
	if overrideSeed != nil {
		seed = *overrideSeed
	}
	if seed == 0 {
		seed = time.Now().UnixNano() % 1000000
	}

	// 2. Determine output directory
	var outputDir string
	if overrideOutputDir != "" {
		outputDir = overrideOutputDir
	} else {
		r := strings.NewReplacer(
			"{seed}", fmt.Sprintf("%d", seed),
			"{run_id}", runID,
		)
		outputDir = r.Replace(cfg.Output.Dir)
		if outputDir == "" {
			outputDir = filepath.Join("runs", fmt.Sprintf("%d", seed))
		}
	}

	// Make sure output dir exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Save a copy of the config inside the run directory with the actual seed set
	cfgCopy := *cfg
	cfgCopy.Seed = seed
	configBytes, err := json.MarshalIndent(&cfgCopy, "", "  ")
	if err == nil {
		_ = os.WriteFile(filepath.Join(outputDir, "config.json"), configBytes, 0644)
	}

	// 3. Initialize SQLite store
	dbPath := filepath.Join(outputDir, "history.sqlite")
	st, err := store.NewStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SQLite store: %w", err)
	}

	// 4. Open events.jsonl file
	jsonlPath := filepath.Join(outputDir, "events.jsonl")
	jsonlFile, err := os.OpenFile(jsonlPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("failed to create events.jsonl: %w", err)
	}

	// 5. Update latest links
	parentDir := filepath.Dir(outputDir)
	latestSymlink := filepath.Join(parentDir, "latest")
	_ = os.Remove(latestSymlink)
	_ = os.Symlink(filepath.Base(outputDir), latestSymlink)
	_ = os.WriteFile(filepath.Join(parentDir, "latest.txt"), []byte(outputDir), 0644)

	return &Runner{
		cfg:       cfg,
		runID:     runID,
		seed:      seed,
		outputDir: outputDir,
		store:     st,
		jsonlFile: jsonlFile,
	}, nil
}

func (r *Runner) GetRunID() string {
	return r.runID
}

func (r *Runner) GetOutputDir() string {
	return r.outputDir
}

func (r *Runner) GetStore() *store.Store {
	return r.store
}

func (r *Runner) calculateConfigHash() string {
	b, err := json.Marshal(r.cfg)
	if err != nil {
		return "unknown"
	}
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:8])
}

// Run executes the full test lifecycle.
func (r *Runner) Run(ctx context.Context) error {
	log.Printf("[Runner] Starting run %s with seed %d\n", r.runID, r.seed)
	log.Printf("[Runner] Output directory: %s\n", r.outputDir)

	configHash := r.calculateConfigHash()

	// 1. Create run record in DB
	runRecord := &model.Run{
		ID:         r.runID,
		Seed:       r.seed,
		StartedAt:  time.Now(),
		Status:     "RUNNING",
		ConfigHash: configHash,
	}
	if err := r.store.CreateRun(runRecord); err != nil {
		return fmt.Errorf("failed to create run record in DB: %w", err)
	}

	r.logEvent(0, "Run", "RunStarted", fmt.Sprintf(
		`{"run_id":"%s","seed":%d,"config_hash":"%s"}`,
		r.runID, r.seed, configHash,
	))

	// 2. Initialize Node Manager
	r.manager = node.NewNodeManager(r.cfg, r.runID, r.outputDir, r.handleNodeEvent)

	// Pre-create node entries in SQLite
	count := r.cfg.System.Nodes.Count
	for i := 1; i <= count; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		port, _ := r.manager.GetPort(nodeID)
		_ = r.store.CreateNode(&model.Node{
			RunID:   r.runID,
			NodeID:  nodeID,
			Status:  string(node.StateDeclared),
			PID:     0,
			Port:    port,
			DataDir: filepath.Join(".failforge", "data", r.runID, nodeID),
		})
	}

	// 3. Initialize & Start Proxy
	r.proxy = proxy.NewProxy(r.cfg.Network.ProxyPort, r.runID, r.manager, r.store, r.logEvent)
	proxyErrChan := make(chan error, 1)
	go func() {
		if err := r.proxy.Start(); err != nil {
			proxyErrChan <- err
		}
	}()

	// Wait briefly for proxy to listen
	time.Sleep(200 * time.Millisecond)

	// 4. Start all cluster nodes
	log.Println("[Runner] Spawning cluster nodes...")
	if err := r.manager.StartAll(ctx); err != nil {
		r.cleanup(runRecord, "CRASHED")
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	// Start faults scheduler
	r.scheduler = faults.NewScheduler(r.cfg, r.runID, r.seed, r.outputDir, r.manager, r.proxy, r.store, r.logEvent)
	if err := r.scheduler.Start(ctx, runRecord.StartedAt); err != nil {
		r.cleanup(runRecord, "CRASHED")
		return fmt.Errorf("failed to start faults scheduler: %w", err)
	}

	// 5. Workload simulation
	duration := time.Duration(r.cfg.Time.DurationMs) * time.Millisecond
	if duration == 0 {
		duration = 5 * time.Second
	}

	wlGen := workload.NewGenerator(r.cfg, r.runID, r.seed, r.store, r.logEvent)
	wlGen.IsBlocked = r.proxy.IsBlocked
	wlGen.GetDelay = r.proxy.GetDelay
	wlCtx, wlCancel := context.WithTimeout(ctx, duration)
	defer wlCancel()

	log.Printf("[Runner] Cluster running. Simulating workload for %v...\n", duration)

	wlDone := make(chan struct{})
	go func() {
		wlGen.Start(wlCtx, runRecord.StartedAt)
		close(wlDone)
	}()

	select {
	case err := <-proxyErrChan:
		log.Printf("[Runner] Proxy crashed: %v\n", err)
		r.cleanup(runRecord, "CRASHED")
		return err
	case <-wlDone:
		log.Println("[Runner] Workload simulation complete.")
	case <-ctx.Done():
		log.Println("[Runner] Context cancelled. Aborting...")
		r.cleanup(runRecord, "ABORTED")
		return ctx.Err()
	}

	// 6. Cleanup & Shutdown
	r.cleanup(runRecord, "PASSED")
	log.Println("[Runner] Run completed successfully.")
	return nil
}

func (r *Runner) cleanup(runRecord *model.Run, finalStatus string) {
	log.Println("[Runner] Shutting down cluster and proxy...")
	if r.scheduler != nil {
		r.scheduler.Stop()
	}
	if r.manager != nil {
		_ = r.manager.StopAll()
	}
	if r.proxy != nil {
		_ = r.proxy.Stop()
	}

	// Run checkers if the run was successful up to this point
	var violationsFound bool
	if finalStatus == "PASSED" && len(r.cfg.Checkers) > 0 {
		log.Println("[Runner] Running consistency and correctness invariant checkers...")
		for _, chkCfg := range r.cfg.Checkers {
			chk, err := checkers.GetChecker(chkCfg.Name)
			if err != nil {
				log.Printf("[Runner] Error getting checker '%s': %v\n", chkCfg.Name, err)
				continue
			}

			violations, err := chk.Check(r.runID, r.store)
			if err != nil {
				log.Printf("[Runner] Checker '%s' failed: %v\n", chkCfg.Name, err)
				continue
			}

			if len(violations) > 0 {
				violationsFound = true
				log.Printf("[Runner] Checker '%s' detected %d violation(s):\n", chkCfg.Name, len(violations))
				for _, v := range violations {
					log.Printf("  - %s: %s\n", v.Severity, v.Description)
					violationRecord := v
					if err := r.store.CreateViolation(&violationRecord); err != nil {
						log.Printf("[Runner] Failed to save violation to DB: %v\n", err)
					}

					// Log Violation event to SQLite and events.jsonl
					payloadMap := map[string]interface{}{
						"checker_name":  v.CheckerName,
						"severity":      v.Severity,
						"description":   v.Description,
						"evidence_json": v.EvidenceJSON,
					}
					payloadBytes, _ := json.Marshal(payloadMap)
					r.logEvent(time.Since(runRecord.StartedAt).Milliseconds(), "Run", "Violation", string(payloadBytes))
				}
			}
		}
	}

	if violationsFound {
		finalStatus = "FAILED"
	}

	// Update run record in DB
	endedAt := time.Now()
	runRecord.EndedAt = &endedAt
	runRecord.Status = finalStatus
	_ = r.store.UpdateRun(runRecord)

	r.logEvent(time.Since(runRecord.StartedAt).Milliseconds(), "Run", "RunCompleted", fmt.Sprintf(
		`{"run_id":"%s","status":"%s"}`,
		r.runID, finalStatus,
	))

	if r.jsonlFile != nil {
		_ = r.jsonlFile.Sync()
		_ = r.jsonlFile.Close()
		r.jsonlFile = nil
	}
}

func (r *Runner) handleNodeEvent(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {
	payloadJSON := "{}"
	if payload != nil {
		payload["node_id"] = nodeID
		if b, err := json.Marshal(payload); err == nil {
			payloadJSON = string(b)
		}
	} else {
		payloadJSON = fmt.Sprintf(`{"node_id":"%s"}`, nodeID)
	}

	r.logEvent(timeMs, "Node", eventType, payloadJSON)

	var status string
	switch eventType {
	case "STARTING_COMPLETED":
		status = string(node.StateRunning)
	case "NodeKilled":
		status = string(node.StateKilled)
	case "NodeStopped":
		status = string(node.StateStopped)
	case "NodeCrashed":
		status = string(node.StateCrashed)
	case "NodePaused":
		status = string(node.StatePaused)
	case "NodeResumed":
		status = string(node.StateRunning)
	default:
		return
	}

	var pid int
	var port int
	if payload != nil {
		if p, ok := payload["pid"].(int); ok {
			pid = p
		} else if pf, ok := payload["pid"].(float64); ok {
			pid = int(pf)
		}
		if p, ok := payload["port"].(int); ok {
			port = p
		} else if pf, ok := payload["port"].(float64); ok {
			port = int(pf)
		}
	}

	// Fetch current nodes and update
	nodes, err := r.store.GetNodes(r.runID)
	if err == nil {
		for _, n := range nodes {
			if n.NodeID == nodeID {
				n.Status = status
				if pid != 0 {
					n.PID = pid
				}
				if port != 0 {
					n.Port = port
				}
				_ = r.store.UpdateNode(n)
				break
			}
		}
	}
}

func (r *Runner) logEvent(timeMs int64, category, eventType, payloadJSON string) {
	e := &model.Event{
		RunID:       r.runID,
		TimeMs:      timeMs,
		Category:    category,
		Type:        eventType,
		PayloadJSON: payloadJSON,
	}
	_ = r.store.CreateEvent(e)

	if r.jsonlFile != nil {
		b, err := json.Marshal(e)
		if err == nil {
			_, _ = r.jsonlFile.Write(append(b, '\n'))
		}
	}
}

type RunResult struct {
	RunID          string
	Status         string
	ViolationCount int
	OutputDir      string
}

func (r *Runner) RunAndReport(ctx context.Context) (*RunResult, error) {
	err := r.Run(ctx)
	
	// Close store to release locks so other processes or the campaign manager can read it.
	r.store.Close()

	// Re-open store to retrieve results
	dbPath := filepath.Join(r.outputDir, "history.sqlite")
	st, storeErr := store.NewStore(dbPath)
	if storeErr != nil {
		return &RunResult{
			RunID:     r.runID,
			Status:    "CRASHED",
			OutputDir: r.outputDir,
		}, fmt.Errorf("failed to re-open store to read results: %w", storeErr)
	}
	defer st.Close()

	runRec, getErr := st.GetRun(r.runID)
	status := "UNKNOWN"
	if getErr == nil && runRec != nil {
		status = runRec.Status
	}

	viols, _ := st.GetViolations(r.runID)
	violCount := 0
	for _, v := range viols {
		if strings.ToUpper(v.Severity) != "INFO" {
			violCount++
		}
	}

	return &RunResult{
		RunID:          r.runID,
		Status:         status,
		ViolationCount: violCount,
		OutputDir:      r.outputDir,
	}, err
}
