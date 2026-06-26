package store

import (
	"database/sql"
	"fmt"
	"time"

	"failforge/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

// NewStore initializes a SQLite database and runs migrations.
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Set busy timeout and enable WAL journal mode for concurrent-safe execution
	_, _ = db.Exec("PRAGMA busy_timeout = 5000;")
	_, _ = db.Exec("PRAGMA journal_mode = WAL;")

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			seed INTEGER NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT,
			status TEXT NOT NULL,
			config_hash TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS nodes (
			run_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			status TEXT NOT NULL,
			pid INTEGER NOT NULL,
			port INTEGER NOT NULL,
			data_dir TEXT NOT NULL,
			PRIMARY KEY (run_id, node_id)
		);`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			time_ms INTEGER NOT NULL,
			category TEXT NOT NULL,
			type TEXT NOT NULL,
			payload_json TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS operations (
			op_id TEXT NOT NULL,
			run_id TEXT NOT NULL,
			client_id TEXT NOT NULL,
			operation TEXT NOT NULL,
			input_json TEXT,
			output_json TEXT,
			start_ms INTEGER,
			end_ms INTEGER,
			status TEXT,
			PRIMARY KEY (run_id, op_id)
		);`,
		`CREATE TABLE IF NOT EXISTS messages (
			message_id TEXT NOT NULL,
			run_id TEXT NOT NULL,
			from_node TEXT NOT NULL,
			to_node TEXT NOT NULL,
			message_type TEXT,
			status TEXT NOT NULL,
			send_ms INTEGER NOT NULL,
			deliver_ms INTEGER,
			payload_hash TEXT,
			PRIMARY KEY (run_id, message_id)
		);`,
		`CREATE TABLE IF NOT EXISTS violations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			checker_name TEXT NOT NULL,
			severity TEXT NOT NULL,
			description TEXT NOT NULL,
			evidence_json TEXT NOT NULL
		);`,
	}

	for _, q := range queries {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// CreateRun inserts a new run into the database.
func (s *Store) CreateRun(run *model.Run) error {
	_, err := s.db.Exec(
		`INSERT INTO runs (id, seed, started_at, status, config_hash) VALUES (?, ?, ?, ?, ?)`,
		run.ID, run.Seed, run.StartedAt.Format(time.RFC3339), run.Status, run.ConfigHash,
	)
	return err
}

// UpdateRun updates ended_at and status of an existing run.
func (s *Store) UpdateRun(run *model.Run) error {
	var endedAt *string
	if run.EndedAt != nil {
		t := run.EndedAt.Format(time.RFC3339)
		endedAt = &t
	}
	_, err := s.db.Exec(
		`UPDATE runs SET ended_at = ?, status = ? WHERE id = ?`,
		endedAt, run.Status, run.ID,
	)
	return err
}

// GetRun retrieves a run by ID.
func (s *Store) GetRun(runID string) (*model.Run, error) {
	row := s.db.QueryRow(`SELECT id, seed, started_at, ended_at, status, config_hash FROM runs WHERE id = ?`, runID)
	var run model.Run
	var startedAtStr string
	var endedAtStr sql.NullString
	if err := row.Scan(&run.ID, &run.Seed, &startedAtStr, &endedAtStr, &run.Status, &run.ConfigHash); err != nil {
		return nil, err
	}

	t, err := time.Parse(time.RFC3339, startedAtStr)
	if err != nil {
		return nil, err
	}
	run.StartedAt = t

	if endedAtStr.Valid {
		et, err := time.Parse(time.RFC3339, endedAtStr.String)
		if err != nil {
			return nil, err
		}
		run.EndedAt = &et
	}

	return &run, nil
}

// CreateNode inserts a new node record.
func (s *Store) CreateNode(n *model.Node) error {
	_, err := s.db.Exec(
		`INSERT INTO nodes (run_id, node_id, status, pid, port, data_dir) VALUES (?, ?, ?, ?, ?, ?)`,
		n.RunID, n.NodeID, n.Status, n.PID, n.Port, n.DataDir,
	)
	return err
}

// UpdateNode updates status and pid of a node.
func (s *Store) UpdateNode(n *model.Node) error {
	_, err := s.db.Exec(
		`UPDATE nodes SET status = ?, pid = ?, port = ? WHERE run_id = ? AND node_id = ?`,
		n.Status, n.PID, n.Port, n.RunID, n.NodeID,
	)
	return err
}

// GetNodes retrieves all nodes for a run.
func (s *Store) GetNodes(runID string) ([]*model.Node, error) {
	rows, err := s.db.Query(`SELECT run_id, node_id, status, pid, port, data_dir FROM nodes WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*model.Node
	for rows.Next() {
		var n model.Node
		if err := rows.Scan(&n.RunID, &n.NodeID, &n.Status, &n.PID, &n.Port, &n.DataDir); err != nil {
			return nil, err
		}
		nodes = append(nodes, &n)
	}
	return nodes, nil
}

// CreateEvent inserts a new event record.
func (s *Store) CreateEvent(e *model.Event) error {
	res, err := s.db.Exec(
		`INSERT INTO events (run_id, time_ms, category, type, payload_json) VALUES (?, ?, ?, ?, ?)`,
		e.RunID, e.TimeMs, e.Category, e.Type, e.PayloadJSON,
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err == nil {
		e.ID = id
	}
	return nil
}

// GetEvents retrieves all events for a run ordered by time_ms.
func (s *Store) GetEvents(runID string) ([]*model.Event, error) {
	rows, err := s.db.Query(`SELECT id, run_id, time_ms, category, type, payload_json FROM events WHERE run_id = ? ORDER BY time_ms ASC, id ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*model.Event
	for rows.Next() {
		var e model.Event
		if err := rows.Scan(&e.ID, &e.RunID, &e.TimeMs, &e.Category, &e.Type, &e.PayloadJSON); err != nil {
			return nil, err
		}
		events = append(events, &e)
	}
	return events, nil
}

// CreateOperation inserts a new operation record.
func (s *Store) CreateOperation(op *model.Operation) error {
	_, err := s.db.Exec(
		`INSERT INTO operations (op_id, run_id, client_id, operation, input_json, output_json, start_ms, end_ms, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		op.OpID, op.RunID, op.ClientID, op.Operation, op.InputJSON, op.OutputJSON, op.StartMs, op.EndMs, op.Status,
	)
	return err
}

// GetOperations retrieves all operations for a run.
func (s *Store) GetOperations(runID string) ([]*model.Operation, error) {
	rows, err := s.db.Query(`SELECT op_id, run_id, client_id, operation, input_json, output_json, start_ms, end_ms, status FROM operations WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var operations []*model.Operation
	for rows.Next() {
		var op model.Operation
		var input, output sql.NullString
		if err := rows.Scan(&op.OpID, &op.RunID, &op.ClientID, &op.Operation, &input, &output, &op.StartMs, &op.EndMs, &op.Status); err != nil {
			return nil, err
		}
		if input.Valid {
			op.InputJSON = input.String
		}
		if output.Valid {
			op.OutputJSON = output.String
		}
		operations = append(operations, &op)
	}
	return operations, nil
}

// CreateMessage inserts a new message log.
func (s *Store) CreateMessage(m *model.Message) error {
	_, err := s.db.Exec(
		`INSERT INTO messages (message_id, run_id, from_node, to_node, message_type, status, send_ms, deliver_ms, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.MessageID, m.RunID, m.FromNode, m.ToNode, m.MessageType, m.Status, m.SendMs, m.DeliverMs, m.PayloadHash,
	)
	return err
}

// UpdateMessage updates status and deliver_ms of an existing message.
func (s *Store) UpdateMessage(m *model.Message) error {
	_, err := s.db.Exec(
		`UPDATE messages SET status = ?, deliver_ms = ? WHERE run_id = ? AND message_id = ?`,
		m.Status, m.DeliverMs, m.RunID, m.MessageID,
	)
	return err
}

// GetMessages retrieves all messages for a run.
func (s *Store) GetMessages(runID string) ([]*model.Message, error) {
	rows, err := s.db.Query(`SELECT message_id, run_id, from_node, to_node, message_type, status, send_ms, deliver_ms, payload_hash FROM messages WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*model.Message
	for rows.Next() {
		var m model.Message
		var msgType, payloadHash sql.NullString
		var deliverMs sql.NullInt64
		if err := rows.Scan(&m.MessageID, &m.RunID, &m.FromNode, &m.ToNode, &msgType, &m.Status, &m.SendMs, &deliverMs, &payloadHash); err != nil {
			return nil, err
		}
		if msgType.Valid {
			m.MessageType = msgType.String
		}
		if payloadHash.Valid {
			m.PayloadHash = payloadHash.String
		}
		if deliverMs.Valid {
			m.DeliverMs = deliverMs.Int64
		}
		messages = append(messages, &m)
	}
	return messages, nil
}

// CreateViolation inserts a violation record.
func (s *Store) CreateViolation(v *model.Violation) error {
	res, err := s.db.Exec(
		`INSERT INTO violations (run_id, checker_name, severity, description, evidence_json) VALUES (?, ?, ?, ?, ?)`,
		v.RunID, v.CheckerName, v.Severity, v.Description, v.EvidenceJSON,
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err == nil {
		v.ID = id
	}
	return nil
}

// GetViolations retrieves violations for a run.
func (s *Store) GetViolations(runID string) ([]*model.Violation, error) {
	rows, err := s.db.Query(`SELECT id, run_id, checker_name, severity, description, evidence_json FROM violations WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var violations []*model.Violation
	for rows.Next() {
		var v model.Violation
		if err := rows.Scan(&v.ID, &v.RunID, &v.CheckerName, &v.Severity, &v.Description, &v.EvidenceJSON); err != nil {
			return nil, err
		}
		violations = append(violations, &v)
	}
	return violations, nil
}
