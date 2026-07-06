package checkers

import "fmt"

var registry = map[string]Checker{
	"read_after_acknowledged_write": &ReadAfterWriteChecker{},
	"lock_exclusivity":              &LockExclusivityChecker{},
	"no_two_leaders":                &LeaderUniquenessChecker{},
	"failed_deploy_protection":      &FailedDeployProtectionChecker{},
	"no_lost_active_volume":         &NoLostActiveVolumeChecker{},
}

func GetChecker(name string) (Checker, error) {
	chk, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown checker: %s", name)
	}
	return chk, nil
}
