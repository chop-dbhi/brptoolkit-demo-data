package main

import (
	"github.com/spf13/cobra"
	"log"
	"sync"
)

var staging sync.WaitGroup

var stagingCmd = &cobra.Command{
	Use: "staging <target>",

	Short: "Perform the staging portion of ETL on specified target.",

	Run: func(cmd *cobra.Command, args []string) {
		stageAll(args)
	},
}

var stageAll = func(args []string) {

	if len(args) == 0 {
		log.Fatal("Error: You must provide a target.")
	}

	staging.Add(2)

	go generateLinkTable(args)
	go redcapStaging(args)

	staging.Wait()
}
