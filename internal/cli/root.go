package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	Verbose bool
)

var RootCmd = &cobra.Command{
	Use:   "failforge",
	Short: "FailForge is a deterministic distributed systems failure testing laboratory",
	Long:  `A local-first framework that runs multi-node systems under controlled crash, partition, and delay events, tracks operations history, and supports seed-based deterministic replays.`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "enable verbose output logging")
}
