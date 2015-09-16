package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tjrivera/go-cap/redcap"
)

type REDCapConfig struct {
	Url      string
	Projects map[string]string
}

var redcapStagingCmd = &cobra.Command{
	Use: "redcap",

	Short: "Run Redcap portion of brp_demoETL",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Fatal("Error: You must provide a target")
		}
		staging.Add(1)
		redcapStaging(args)
	}}

var redcapStaging = func(args []string) {

	defer staging.Done()

	var Rc REDCapConfig

	err := viper.UnmarshalKey("REDCap", &Rc)

	if err != nil {
		log.Println("unable to decode: %s", err)
	}

	target := targets[args[0]]

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target.Host,
		target.User,
		target.Db))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	for _, token := range Rc.Projects {

		// Create REDCap project
		project := redcap.NewRedcapProject(Rc.Url, token, true)

		// Wait group for concurrent form retrieval
		var form_retrieval sync.WaitGroup
		form_retrieval.Add(len(project.Forms))

		// Drop existing tables
		for _, form := range project.Forms {
			_, err := db.Query(fmt.Sprintf("drop table if exists %s", form.Name))
			if err != nil {
				log.Fatal(err)
			}
		}

		// Create REDCap base tables
		_, err = db.Query(string(project.ToSQL("postgres")))
		if err != nil {
			log.Fatal("[redcap] error creating base redcap tables. ", err)
		}

		// Export records for project forms
		for _, form := range project.Forms {
			// Ignore legacy tables
			if !strings.Contains(form.Name, "old") {
				// Send concurrent REDCap API request
				go func(f *redcap.RedcapForm) {
					defer form_retrieval.Done()

					params := redcap.ExportParameters{
						Fields:              []string{project.Unique_key.Field_name, "redcap_event_name"},
						Forms:               []string{f.Name},
						RawOrLabel:          "label",
						Format:              "csv",
						ExportCheckboxLabel: true,
					}

					res := project.ExportRecords(params)

					r := bytes.NewReader(res)
					// Persist to Postgres
					txn, err := db.Begin()
					if err != nil {
						log.Fatal(err)
					}

					if err != nil {
						log.Fatal("[redcap][database] unable to start transaction",err)
					}

					reader := csv.NewReader(r)
					csvData, err := reader.ReadAll()

					if err != nil {
						log.Fatal("[redcap] unable to read csv", err)
					}

					// Prepare INSERT statements from CSV file
					for i, line := range csvData {
						// skip header line
						if i == 0 {
							continue
						}

						for j, value := range line {
							// Escape quoted characters and convert to NULL if empty
							line[j] = prepareValue(value)
						}

						values := strings.Join(line, ",")

						stmt, err := txn.Prepare(fmt.Sprintf("insert into %s values(%s)", f.Name, values))
						if err != nil {
							log.Fatal(fmt.Sprintf("[redcap] error formatting insert statement for table %s. ", f.Name), err)
						}

						if stmt != nil {
							_, err = stmt.Exec()
							if err != nil {
								log.Fatal("[redcap] error executing statement ", err)
							}
							err = stmt.Close()
						}
					}
					err = txn.Commit()
					log.Printf("Loaded REDCap form \"%s\"\n", f.Name)
					if err != nil {
						log.Fatal("[redcap][database] unable to commit form to table",err)
					}
				}(form)
			} else {
				form_retrieval.Done()
			}
		}
		form_retrieval.Wait()
		log.Println("REDCap staging completed.")
	}
}

var redcapTransform = func(args []string) {
	target_config := targets[args[0]]

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	stmt, _ := ioutil.ReadFile("transform/REDCapTransform.sql")
	_, err = db.Exec(string(stmt))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("REDCap transforms complete.")
}

var prepareValue = func(s string) string {
	re := regexp.MustCompile("'")
	s = re.ReplaceAllString(s, "''")
	if s == "" {
		return "NULL"
	} else {
		return fmt.Sprintf("'%s'", s)
	}

}
