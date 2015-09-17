package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
)

type DatabaseConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Db           string
	Ssh_user     string
	Ssh_password string
}

var mainCmd = &cobra.Command{
	Use: "brptoolkit-demo-data",

	Short: "brptoolkit-demo-data ETL",

	Long: "Biorepository Toolkit Demo ETL and data processes CLI",

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var etlCmd = &cobra.Command{
	Use: "etl",

	Short: "Perform both staging and transforms functions.",

	Run: func(cmd *cobra.Command, args []string) {
		stageAll(args)
		transformAll(args)
		cleanUp(args)
	},
}

var targets = map[string]DatabaseConfig{}
var sources = map[string]DatabaseConfig{}

func main() {

	// Run Staging Commands
	mainCmd.AddCommand(stagingCmd)
	// Run Cleanup Command
	mainCmd.AddCommand(cleanupCmd)

	// Perform REDCap Staging
	stagingCmd.AddCommand(redcapStagingCmd)
	// Perform Nautilus Staging
	stagingCmd.AddCommand(nautilusStagingCmd)
	// Perform eHB Staging
	stagingCmd.AddCommand(ehbStagingCmd)

	// Perform Transform of staging data
	mainCmd.AddCommand(transformCmd)

	// Stage and transform the data.
	mainCmd.AddCommand(etlCmd)

	viper.SetEnvPrefix("brp_demo")
	viper.AutomaticEnv()
	viper.SetConfigName("config")

	// Directory the program is being called from
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))

	if err == nil {
		viper.AddConfigPath(dir)
	}

	viper.ReadInConfig()

	err = viper.UnmarshalKey("targets", &targets)
	if err != nil {
		log.Println(err)
	}
	err = viper.UnmarshalKey("sources", &sources)
	if err != nil {
		log.Println(err)
	}

	mainCmd.Execute()

}
