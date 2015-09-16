/*
This package will request details from the BRP API and create
an ehb_link table with the results of the request.
*/

package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type BRPConfig struct {
	Url       string
	Protocols []int
	Token     string
}

type DataSource struct {
	Id              int64
	Name            string
	Url             string
	DescriptionHelp string `json:"desc_help"`
	Description     string
	EhbServiceEsId  int64 `json:"ehb_service_es_id"`
}

type ProtocolDataSource struct {
	Id                   int64
	Protocol             string
	DataSource           DataSource
	Path                 string
	Driver               int64
	DriverConfiguration  interface{} `json:"driver_configuration"`
	DisplayLabel         string      `json:"display_label"`
	MaxRecordsPerSubject int64       `json:"max_records_per_subject"`
	Authorized           bool
}

type ApiSubjects struct {
	Count    int64
	Subjects []Subject
}

type Subject struct {
	Id                    int64
	FirstName             string `json:"first_name"`
	LastName              string `json:"last_name"`
	OrganizationId        int64  `json:"organization_id"`
	OrganizationSubjectId string `json:"organization_subject_id"`
	Dob                   string
	Modified              string
	Created               string
	ExternalRecords       []ExternalRecord `json:"external_records"`
}

type ExternalRecord struct {
	RecordId         string `json:"record_id"`
	SubjectId        int64  `json:"subject_id"`
	ExternalSystemId int64  `json:"external_system_id"`
	Modified         string
	Created          string
	Path             string
	Id               int64
	LabelId          int64 `json:"label_id"`
}

var Bc = BRPConfig{}
var w sync.WaitGroup
var count = 0

// Create an insecure transport because eHB not currently providing proper certs
var tr = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}
var client = &http.Client{Transport: tr}

var ehbStagingCmd = &cobra.Command{
	Use: "ehb",

	Short: "eHB Staging ETL",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Fatal("Error: You must provide a target.")
		}
		staging.Add(1)
		generateLinkTable(args)
	},
}

var brpAPIRequest = func(url string) []byte {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("token %s", Bc.Token))

	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	return body
}

var getSubjects = func(d ProtocolDataSource, c chan []Subject) {

	var as ApiSubjects
	url := fmt.Sprintf("%sprotocoldatasources/%d/subjects/", Bc.Url, d.Id)
	r := brpAPIRequest(url)
	err := json.Unmarshal(r, &as)
    if err != nil {
		log.Fatal(err)
	}
	c <- as.Subjects
}

var generateLinkTable = func(args []string) {

	defer staging.Done()

	target_config := targets[args[0]]

	err := viper.UnmarshalKey("BRP", &Bc)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	stmt, _ := ioutil.ReadFile("ddl/ehb_link.sql")
	_, err = db.Query(string(stmt))
	if err != nil {
		log.Fatal(err)
	}

	var datasources []ProtocolDataSource

	// Get all DataSources associated with Protocols
	for _, protocol := range Bc.Protocols {
		var ds []ProtocolDataSource
		url := fmt.Sprintf("%sprotocols/%d/data_sources/", Bc.Url, protocol)
		r := brpAPIRequest(url)
		err := json.Unmarshal(r, &ds)
		if err != nil {
			log.Fatal(err)
		}
		datasources = append(datasources, ds...)
	}

	subjects_queue := make(chan []Subject)

	for _, ds := range datasources {
		go getSubjects(ds, subjects_queue)
	}

	// Start DB Transaction
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	var c = len(datasources)

	for subjects := range subjects_queue {
		for _, subject := range subjects {
			for _, record := range subject.ExternalRecords {
                stmt, err := txn.Prepare(fmt.Sprintf("insert into ehb_link (ehb_id, external_system_id, external_id, organization_id, organization_subject_id, created, dob) values (%d, %d, '%s', %d, '%s', '%s', '%s')",
					subject.Id,
					record.ExternalSystemId,
					record.RecordId,
					subject.OrganizationId,
					subject.OrganizationSubjectId,
					record.Created,
					subject.Dob))
				if err != nil {
					log.Fatal(err)
				}
				_, err = stmt.Exec()
				if err != nil {
					log.Fatal(err)
				}
				err = stmt.Close()
			}
		}
		c -= 1
		if c == 0 {
			close(subjects_queue)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("eHB Link Table Complete.")
}

var transformLinkTable = func(args []string) {

	log.Println("Running transforms on eHB link table.")

	target_config := targets[args[0]]

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	stmt, _ := ioutil.ReadFile("transform/eHBTransform.sql")
	_, err = db.Query(string(stmt))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Link table transforms complete.")

}
