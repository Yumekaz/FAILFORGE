package checkers

import (
	"failforge/internal/model"
	"failforge/internal/store"
)

type Checker interface {
	Name() string
	Check(runID string, st *store.Store) ([]model.Violation, error)
}
