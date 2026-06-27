package faults

import (
	"context"
	"time"

	"failforge/internal/config"
	"failforge/internal/node"
	"failforge/internal/proxy"
	"failforge/internal/store"
)

// Fault is the interface all fault types must implement.
type Fault interface {
	Type() string
	Validate(cfg *config.FaultConfig) error
	Inject(ctx context.Context, fctx *FaultContext) error
}

// FaultContext provides dependencies a fault needs during injection.
type FaultContext struct {
	Config    *config.FaultConfig
	Manager   *node.NodeManager
	Proxy     *proxy.Proxy
	Store     *store.Store
	RunDir    string // output directory for the run
	Seed      int64
	LogEvent  func(timeMs int64, category, eventType, payloadJSON string)
	StartTime time.Time
}
