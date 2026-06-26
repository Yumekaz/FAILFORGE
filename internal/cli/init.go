package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var defaultTemplate = `# FailForge default test configuration
name: default-toy-kv-test
seed: 42

time:
  duration_ms: 10000
  tick_ms: 10

system:
  type: process_cluster
  nodes:
    count: 3
    # Command to run each node process. Placeholders are replaced dynamically.
    command: "python3 node.py --id {node_id} --port {port} --proxy {proxy_url}"
    ports:
      start: 7000
    data_dir: ".failforge/data/{run_id}/{node_id}"

network:
  mode: controlled_proxy
  proxy_port: 9000

workload:
  type: kv-register
  clients: 2
  duration_ms: 8000
  keys: [x]
  operations:
    put:
      weight: 5
    get:
      weight: 5

faults:
  mode: seeded_random
  profile:
    max_faults: 3
    kill_node:
      weight: 1
    restart_node:
      weight: 1
    partition:
      weight: 2

checkers:
  - name: read_after_acknowledged_write

output:
  dir: "runs/{seed}"
`

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a default FailForge configuration",
	Long:  `Creates a failforge.yml file in the current directory with basic example configurations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := "failforge.yml"
		if _, err := os.Stat(filename); err == nil {
			return fmt.Errorf("file %s already exists", filename)
		}

		if err := os.WriteFile(filename, []byte(defaultTemplate), 0644); err != nil {
			return fmt.Errorf("failed to write config template: %w", err)
		}

		fmt.Printf("Initialized default configuration template in %s\n", filename)
		return nil
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
}
