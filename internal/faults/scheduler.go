package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/node"
	"failforge/internal/proxy"
	"failforge/internal/store"
)

type Scheduler struct {
	cfg        *config.Config
	runID      string
	seed       int64
	outputDir  string
	manager    *node.NodeManager
	proxy      *proxy.Proxy
	store      *store.Store
	onEvent    func(timeMs int64, category, eventType, payloadJSON string)
	startTime  time.Time
	schedule   []config.FaultConfig
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

func NewScheduler(
	cfg *config.Config,
	runID string,
	seed int64,
	outputDir string,
	manager *node.NodeManager,
	proxy *proxy.Proxy,
	store *store.Store,
	onEvent func(timeMs int64, category, eventType, payloadJSON string),
) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		runID:     runID,
		seed:      seed,
		outputDir: outputDir,
		manager:   manager,
		proxy:     proxy,
		store:     store,
		onEvent:   onEvent,
	}
}

func (s *Scheduler) Start(ctx context.Context, startTime time.Time) error {
	s.startTime = startTime

	// 1. Resolve schedule
	if strings.ToLower(s.cfg.Faults.Mode) == "scripted" {
		s.schedule = s.cfg.Faults.Schedule
	} else if strings.ToLower(s.cfg.Faults.Mode) == "seeded_random" {
		s.schedule = s.generateRandomSchedule()
		// Save generated faults schedule to runs/<run_id>/faults.json
		faultsJSON, err := json.MarshalIndent(s.schedule, "", "  ")
		if err == nil {
			_ = os.WriteFile(filepath.Join(s.outputDir, "faults.json"), faultsJSON, 0644)
		}
	} else if strings.ToLower(s.cfg.Faults.Mode) == "replay" {
		// Load from faults.json in output directory
		data, err := os.ReadFile(filepath.Join(s.outputDir, "faults.json"))
		if err != nil {
			return fmt.Errorf("replay mode failed to read faults.json: %w", err)
		}
		if err := json.Unmarshal(data, &s.schedule); err != nil {
			return fmt.Errorf("failed to parse faults.json: %w", err)
		}
	}

	// Sort schedule by AtMs ascending
	sort.Slice(s.schedule, func(i, j int) bool {
		return s.schedule[i].AtMs < s.schedule[j].AtMs
	})

	if len(s.schedule) == 0 {
		log.Println("[Scheduler] No faults scheduled.")
		return nil
	}

	log.Printf("[Scheduler] Loaded %d scheduled faults.\n", len(s.schedule))

	schedCtx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	s.wg.Add(1)
	go s.runLoop(schedCtx)

	return nil
}

func (s *Scheduler) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
}

func (s *Scheduler) runLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(s.startTime).Milliseconds()
			for idx < len(s.schedule) && s.schedule[idx].AtMs <= elapsed {
				fault := s.schedule[idx]
				s.injectFault(ctx, fault)
				idx++
			}
			if idx >= len(s.schedule) {
				// Finished injecting all scheduled faults
				return
			}
		}
	}
}

func (s *Scheduler) injectFault(ctx context.Context, f config.FaultConfig) {
	timeMs := time.Since(s.startTime).Milliseconds()

	payloadBytes, _ := json.Marshal(f)
	payloadJSON := string(payloadBytes)

	// Log event to SQLite timeline
	s.onEvent(timeMs, "Fault", f.Type, payloadJSON)

	// Record violation/evidence of fault injection in DB
	_ = s.store.CreateViolation(&model.Violation{
		RunID:        s.runID,
		CheckerName:  "fault_injector",
		Severity:     "info",
		Description:  fmt.Sprintf("Injected network/process fault of type: %s", f.Type),
		EvidenceJSON: payloadJSON,
	})

	log.Printf("[Scheduler] [%dms] Injecting fault %s: %s\n", timeMs, f.Type, payloadJSON)

	switch strings.ToLower(f.Type) {
	case "kill_node":
		if f.Node != "" {
			_ = s.manager.KillNode(f.Node)
		}
	case "restart_node":
		if f.Node != "" {
			_ = s.manager.RestartNode(ctx, f.Node)
		}
	case "partition":
		if len(f.Groups) > 0 {
			s.proxy.SetPartition(f.Groups)
		}
	case "heal":
		s.proxy.ClearPartitions()
		s.proxy.ClearFaultRules()
	case "delay_messages":
		if f.From != "" && f.To != "" && f.DelayMs > 0 {
			s.proxy.SetDelayRule(f.From, f.To, time.Duration(f.DelayMs)*time.Millisecond)
		}
	case "drop_messages":
		if f.From != "" && f.To != "" {
			s.proxy.SetDropRule(f.From, f.To, true)
		}
	}
}

func (s *Scheduler) generateRandomSchedule() []config.FaultConfig {
	r := rand.New(rand.NewSource(s.seed))

	maxFaults := 3
	if val, ok := s.cfg.Faults.Profile["max_faults"]; ok {
		if vi, ok := val.(int); ok {
			maxFaults = vi
		} else if vf, ok := val.(float64); ok {
			maxFaults = int(vf)
		}
	}

	var keys []string
	for name := range s.cfg.Faults.Profile {
		if name == "max_faults" {
			continue
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)

	type FaultTypeWeight struct {
		name   string
		weight int
	}
	var weights []FaultTypeWeight
	totalWeight := 0
	for _, name := range keys {
		val := s.cfg.Faults.Profile[name]
		weight := 0
		switch wVal := val.(type) {
		case int:
			weight = wVal
		case float64:
			weight = int(wVal)
		case map[string]interface{}:
			if wt, ok := wVal["weight"]; ok {
				if wti, ok := wt.(int); ok {
					weight = wti
				} else if wtf, ok := wt.(float64); ok {
					weight = int(wtf)
				}
			}
		case map[interface{}]interface{}:
			if wt, ok := wVal["weight"]; ok {
				if wti, ok := wt.(int); ok {
					weight = wti
				} else if wtf, ok := wt.(float64); ok {
					weight = int(wtf)
				}
			}
		}
		if weight > 0 {
			weights = append(weights, FaultTypeWeight{name: name, weight: weight})
			totalWeight += weight
		}
	}

	if totalWeight == 0 {
		weights = []FaultTypeWeight{
			{name: "kill_node", weight: 2},
			{name: "restart_node", weight: 2},
			{name: "partition", weight: 3},
			{name: "heal", weight: 3},
		}
		totalWeight = 10
	}

	durationMs := int64(s.cfg.Time.DurationMs)
	if durationMs == 0 {
		durationMs = 5000
	}

	startBuffer := int64(500)
	endBuffer := int64(1000)
	usableDuration := durationMs - startBuffer - endBuffer
	if usableDuration <= 0 {
		usableDuration = 100
		startBuffer = 0
	}

	segment := usableDuration / int64(maxFaults)
	var generated []config.FaultConfig
	nodeCount := s.cfg.System.Nodes.Count
	if nodeCount <= 0 {
		nodeCount = 3
	}

	activePartitions := false

	for i := 0; i < maxFaults; i++ {
		segmentStart := startBuffer + int64(i)*segment
		atMs := segmentStart + r.Int63n(segment)

		rVal := r.Intn(totalWeight)
		curr := 0
		selectedType := "heal"
		for _, w := range weights {
			curr += w.weight
			if rVal < curr {
				selectedType = w.name
				break
			}
		}

		fault := config.FaultConfig{
			AtMs: atMs,
			Type: selectedType,
		}

		randomNode := fmt.Sprintf("node-%d", r.Intn(nodeCount)+1)

		switch selectedType {
		case "kill_node", "restart_node":
			fault.Node = randomNode
		case "partition":
			var g1, g2 []string
			for n := 1; n <= nodeCount; n++ {
				nodeID := fmt.Sprintf("node-%d", n)
				if r.Intn(2) == 0 {
					g1 = append(g1, nodeID)
				} else {
					g2 = append(g2, nodeID)
				}
			}
			if len(g1) == 0 {
				g1 = append(g1, fmt.Sprintf("node-%d", r.Intn(nodeCount)+1))
			}
			// ensure g2 is not empty by matching remaining nodes
			for n := 1; n <= nodeCount; n++ {
				nodeID := fmt.Sprintf("node-%d", n)
				found := false
				for _, val := range g1 {
					if val == nodeID {
						found = true
						break
					}
				}
				if !found {
					g2 = append(g2, nodeID)
				}
			}
			if len(g2) == 0 {
				g1 = []string{"node-1"}
				g2 = []string{"node-2", "node-3"}
			}
			fault.Groups = [][]string{g1, g2}
			activePartitions = true
		case "heal":
			activePartitions = false
		case "delay_messages":
			fault.From = "client"
			fault.To = randomNode
			fault.DelayMs = 100 + r.Intn(400)
		case "drop_messages":
			fault.From = "client"
			fault.To = randomNode
		}

		generated = append(generated, fault)
	}

	if activePartitions {
		generated = append(generated, config.FaultConfig{
			AtMs: durationMs - 500,
			Type: "heal",
		})
	}

	return generated
}
