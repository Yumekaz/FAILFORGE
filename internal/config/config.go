package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Name     string          `yaml:"name"`
	Seed     int64           `yaml:"seed,omitempty"`
	Time     TimeConfig      `yaml:"time"`
	System   SystemConfig    `yaml:"system"`
	Network  NetworkConfig   `yaml:"network"`
	Workload WorkloadConfig  `yaml:"workload"`
	Faults   FaultsConfig    `yaml:"faults"`
	Checkers []CheckerConfig `yaml:"checkers"`
	Output   OutputConfig    `yaml:"output"`
}

type TimeConfig struct {
	DurationMs int `yaml:"duration_ms"`
	TickMs     int `yaml:"tick_ms"`
}

type SystemConfig struct {
	Type  string      `yaml:"type"`
	Nodes NodesConfig `yaml:"nodes"`
}

type NodesConfig struct {
	Count   int         `yaml:"count"`
	Command string      `yaml:"command"`
	Ports   PortsConfig `yaml:"ports"`
	DataDir string      `yaml:"data_dir"`
}

type PortsConfig struct {
	Start int `yaml:"start"`
}

type NetworkConfig struct {
	Mode      string `yaml:"mode"`
	ProxyPort int    `yaml:"proxy_port"`
}

type WorkloadConfig struct {
	Type       string                 `yaml:"type"`
	Clients    int                    `yaml:"clients"`
	DurationMs int                    `yaml:"duration_ms"`
	Keys       []string               `yaml:"keys,omitempty"`
	Operations map[string]interface{} `yaml:"operations"`
}

type FaultConfig struct {
	AtMs    int64      `yaml:"at_ms" json:"at_ms"`
	Type    string     `yaml:"type" json:"type"`
	Node    string     `yaml:"node,omitempty" json:"node,omitempty"`
	Groups  [][]string `yaml:"groups,omitempty" json:"groups,omitempty"`
	From    string     `yaml:"from,omitempty" json:"from,omitempty"`
	To      string     `yaml:"to,omitempty" json:"to,omitempty"`
	DelayMs int        `yaml:"delay_ms,omitempty" json:"delay_ms,omitempty"`
}

type FaultsConfig struct {
	Mode     string                 `yaml:"mode"`
	Schedule []FaultConfig          `yaml:"schedule,omitempty"`
	Profile  map[string]interface{} `yaml:"profile,omitempty"`
}

type CheckerConfig struct {
	Name string `yaml:"name"`
}

type OutputConfig struct {
	Dir string `yaml:"dir"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
