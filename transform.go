package main

import (
	"github.com/spf13/cobra"
	"log"
)

var transformCmd = &cobra.Command{
	Use: "transform <target>",

	Short: "Perform the transform portion of ETL on specified target.",

	Run: func(cmd *cobra.Command, args []string) {

		transformAll(args)
	},
}

var transformAll = func(args []string) {
	if len(args) == 0 {
		log.Fatal("Error: You must provide a target.")
	}

	transformLinkTable(args)
	redcapTransform(args)

}
