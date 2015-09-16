package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"log"
)

var cleanupCmd = &cobra.Command{
	Use: "cleanup",

	Short: "Run cleanup portion of brp_demoETL",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Fatal("Error: You must provide a target")
		}
		cleanUp(args)
	}}

var cleanUp = func(args []string) {
	target_config := targets[args[0]]
	// Open up our connection to the target DB
	target, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	defer target.Close()

	qs := `
		drop table ehb_link; 
		drop table ehb_link_md5;
	`

	_, err = target.Query(qs)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Removed staging tables")
}
