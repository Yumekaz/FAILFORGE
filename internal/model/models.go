package model

import "time"

// Run represents a single execution run of a test profile.
type Run struct {
	ID         string     `json:"id"`
	Seed       int64      `json:"seed"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	Status     string     `json:"status"` // CREATED, CONFIG_LOADED, RUNNING, PASSED, FAILED, CRASHED, ABORTED
	ConfigHash string     `json:"config_hash"`
}

// Node represents a process-based node in a run.
type Node struct {
	RunID   string `json:"run_id"`
	NodeID  string `json:"node_id"`
	Status  string `json:"status"` // DECLARED, STARTING, RUNNING, PAUSED, KILLED, RESTARTING, STOPPED, CRASHED
	PID     int    `json:"pid"`
	Port    int    `json:"port"`
	DataDir string `json:"data_dir"`
}

// Event is a generic structured timeline log event.
type Event struct {
	ID          int64  `json:"id"`
	RunID       string `json:"run_id"`
	TimeMs      int64  `json:"time_ms"`
	Category    string `json:"category"`     // Run, Node, Fault, Message, Operation, SystemLog
	Type        string `json:"type"`         // e.g. NodeStarted, MessageDropped, OperationCompleted
	PayloadJSON string `json:"payload_json"` // JSON representation of event metadata
}

// Operation represents a client-side execution operation against the cluster.
type Operation struct {
	OpID       string `json:"op_id"`
	RunID      string `json:"run_id"`
	ClientID   string `json:"client_id"`
	Operation  string `json:"operation"` // e.g. PUT, GET, lock, release
	InputJSON  string `json:"input_json"`
	OutputJSON string `json:"output_json"`
	StartMs    int64  `json:"start_ms"`
	EndMs      int64  `json:"end_ms"`
	Status     string `json:"status"` // ok, fail, info
}

// Message represents a node-to-node message intercepted by the proxy.
type Message struct {
	MessageID   string `json:"message_id"`
	RunID       string `json:"run_id"`
	FromNode    string `json:"from_node"`
	ToNode      string `json:"to_node"`
	MessageType string `json:"message_type,omitempty"`
	Status      string `json:"status"` // sent, delivered, dropped, delayed
	SendMs      int64  `json:"send_ms"`
	DeliverMs   int64  `json:"deliver_ms,omitempty"`
	PayloadHash string `json:"payload_hash,omitempty"`
}

// Violation represents a correctness invariant violation detected in history.
type Violation struct {
	ID           int64  `json:"id"`
	RunID        string `json:"run_id"`
	CheckerName  string `json:"checker_name"`
	Severity     string `json:"severity"` // info, warning, error, fatal
	Description  string `json:"description"`
	EvidenceJSON string `json:"evidence_json"`
}
