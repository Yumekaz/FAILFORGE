package faults

import (
	"strings"
)

type Registry struct {
	faults map[string]Fault
}

func NewRegistry() *Registry {
	return &Registry{
		faults: make(map[string]Fault),
	}
}

func (r *Registry) Register(f Fault) {
	typeName := strings.ToLower(f.Type())
	r.faults[typeName] = f
}

func (r *Registry) Get(typeName string) (Fault, bool) {
	typeName = strings.ToLower(typeName)
	f, ok := r.faults[typeName]
	return f, ok
}

func (r *Registry) AllTypes() []string {
	var types []string
	for k := range r.faults {
		types = append(types, k)
	}
	return types
}

// DefaultRegistry returns a registry populated with all standard and advanced faults.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	
	// Legacy faults
	r.Register(&KillNodeFault{})
	r.Register(&RestartNodeFault{})
	r.Register(&PartitionFault{})
	r.Register(&HealFault{})
	r.Register(&DelayMessagesFault{})
	r.Register(&DropMessagesFault{})

	// Advanced faults
	r.Register(&AsymmetricPartitionFault{})
	r.Register(&DuplicateMessagesFault{})
	r.Register(&CorruptMessagesFault{})
	r.Register(&CpuPauseFault{})
	r.Register(&SlowDiskFault{})
	r.Register(&DiskWriteLossFault{})
	r.Register(&PartialPersistenceFault{})
	r.Register(&StaleSnapshotRestartFault{})
	r.Register(&ClockSkewFault{})

	return r
}
