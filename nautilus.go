package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq" // For use of NullTime
	_ "github.com/mattn/go-oci8"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

type NautilusSDG struct {
	Sdg_id            string
	Sample_subject_id string
	Exref             sql.NullString
	Collection_site   sql.NullString
	Universal_id      string
}

type NautilusVisit struct {
	Visit_id          string
	Sample_subject_id string
	Sdg_id            string
	Received_on       time.Time
	Sd_group_name     string
	Sd_visit_name     string
	Visit_description sql.NullString
	Visit_time_date   time.Time
}

type NautilusAliquot struct {
	SdgId                sql.NullInt64
	SampleSubjectName    sql.NullString
	CollectionSite       sql.NullString
	PotentialUniversalId sql.NullString
	VisitName            sql.NullString
	AliquotId            int
	AliquotName          sql.NullString
	ParentAliquotId      sql.NullInt64
	VisitId              int
	ReceivedOn           pq.NullTime
	SampleTypeCode       sql.NullString
	SecondarySampleCode  sql.NullString
	SampleType           sql.NullString
	SecondarySampleType  sql.NullString
	FullSampleTypeDesc   sql.NullString
	CollectionEventName  sql.NullString
	DrawNote             sql.NullString
	TissueType           sql.NullString
	SpecimenCategory     sql.NullString
	CollectMethod        sql.NullString
	ReceivedDateTime     pq.NullTime
	VolumeReceived       sql.NullFloat64
	VolumeRemaining      sql.NullFloat64
	VolumeUnits          sql.NullString
	Concentration        sql.NullFloat64
	ConcentrationUnits   sql.NullString
	UnitId               sql.NullInt64
	DisposedFlag         sql.NullString
	AvailableFlag        sql.NullString // Can probably get rid of one of these as they are mutually exclusive.
	Disposed             sql.NullString
	LocationId           sql.NullInt64
	CollectDateTime      pq.NullTime
}

var dbSync sync.WaitGroup

const layout = "Mon Jan _2 15:04:05 MST 2006"

var nautilusStagingCmd = &cobra.Command{
	Use: "nautilus",

	Short: "Run Nautilus portion of brp_demoETL",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Fatal("Error: You must provide a target.")
		}
		staging.Add(1)
		nautilusStaging(args)
	}}

var nautilusStaging = func(args []string) {

	defer staging.Done()

	nautilus := sources["nautilus"]

	target_config := targets[args[0]]

	source, err := sql.Open("oci8", fmt.Sprintf("%s/%s@%s:%d/%s",
		nautilus.User,
		nautilus.Password,
		nautilus.Host,
		nautilus.Port,
		nautilus.Db))

	if err != nil {
		log.Println(err)
	}

	sdg_c := make(chan []NautilusSDG)
	visit_c := make(chan []NautilusVisit)
	aliquot_c := make(chan []NautilusAliquot)

	// Retrieve Source Data
	go retrieveNautilusSDGs(source, sdg_c)
	go retrieveNautilusVisits(source, visit_c)
	go retrieveNautilusAliquots(source, aliquot_c)

	defer source.Close()

	// Create connection to target (we assume postgres)
	target, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	// Drop/Create Nautilus Tables on Target
	qs, err := ioutil.ReadFile("ddl/nautilus.sql")
	_, err = target.Query(string(qs))
	if err != nil {
		log.Fatal(err)
	}

	dbSync.Add(3)

	// Persist to target
	go persistNautilusSDGs(target, sdg_c)
	go persistNautilusVisits(target, visit_c)
	go persistNautilusAliquots(target, aliquot_c)

	dbSync.Wait()

	defer target.Close()

	log.Println("Nautilus Staging Completed.")

}

var retrieveNautilusSDGs = func(db *sql.DB, c chan []NautilusSDG) {

	var sdgs []NautilusSDG

	qs, err := ioutil.ReadFile("extract/NautilusSDG.sql")

	rows, err := db.Query(string(qs))
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		var sdg NautilusSDG
		err := rows.Scan(
			&sdg.Sdg_id,
			&sdg.Sample_subject_id,
			&sdg.Exref,
			&sdg.Collection_site,
			&sdg.Universal_id,
		)
		if err != nil {
			log.Fatal(err)
		}
		sdgs = append(sdgs, sdg)
	}
	c <- sdgs
	log.Println("Loaded Nautilus SDGs")
}

var retrieveNautilusVisits = func(db *sql.DB, c chan []NautilusVisit) {

	var visits []NautilusVisit

	// Read NautilusVisit SQL
	qs, err := ioutil.ReadFile("extract/NautilusVisit.sql")
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve visits from DB.
	rows, err := db.Query(string(qs))
	if err != nil {
		log.Println(err)
	}
	// Scan to struct
	for rows.Next() {
		var visit NautilusVisit
		err := rows.Scan(
			&visit.Visit_id,
			&visit.Sample_subject_id,
			&visit.Sdg_id,
			&visit.Received_on,
			&visit.Sd_group_name,
			&visit.Sd_visit_name,
			&visit.Visit_description,
			&visit.Visit_time_date,
		)
		if err != nil {
			log.Fatal(err)
		}
		visits = append(visits, visit)
	}
	c <- visits
	log.Println("Loaded Nautilus Visits")
}

var retrieveNautilusAliquots = func(db *sql.DB, c chan []NautilusAliquot) {

	var aliquots []NautilusAliquot

	qs, err := ioutil.ReadFile("extract/NautilusAliquot.sql")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(string(qs))
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var aliquot NautilusAliquot
		err := rows.Scan(
			&aliquot.SdgId,
			&aliquot.SampleSubjectName,
			&aliquot.CollectionSite,
			&aliquot.PotentialUniversalId,
			&aliquot.VisitName,
			&aliquot.AliquotId,
			&aliquot.AliquotName,
			&aliquot.ParentAliquotId,
			&aliquot.VisitId,
			&aliquot.ReceivedOn,
			&aliquot.SampleTypeCode,
			&aliquot.SecondarySampleCode,
			&aliquot.SampleType,
			&aliquot.SecondarySampleType,
			&aliquot.FullSampleTypeDesc,
			&aliquot.CollectionEventName,
			&aliquot.DrawNote,
			&aliquot.TissueType,
			&aliquot.SpecimenCategory,
			&aliquot.CollectMethod,
			&aliquot.ReceivedDateTime,
			&aliquot.VolumeReceived,
			&aliquot.VolumeRemaining,
			&aliquot.VolumeUnits,
			&aliquot.Concentration,
			&aliquot.ConcentrationUnits,
			&aliquot.UnitId,
			&aliquot.DisposedFlag,
			&aliquot.AvailableFlag,
			&aliquot.Disposed,
			&aliquot.LocationId,
			&aliquot.CollectDateTime,
		)
		if err != nil {
			log.Fatal(err)
		}
		aliquots = append(aliquots, aliquot)

	}
	c <- aliquots
	log.Println("Loaded Nautilus Aliquots")
}

var persistNautilusSDGs = func(db *sql.DB, c chan []NautilusSDG) {

	defer dbSync.Done()

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, sdg := range <-c {
		stmt, err := txn.Prepare(`insert into nautilus_sdg_staging values($1, $2, $3, $4, $5)`)
		if err != nil {
			log.Fatal(err)
		}
		if stmt != nil {
			_, err := stmt.Exec(
				sdg.Sdg_id,
				sdg.Sample_subject_id,
				sdg.Exref,
				sdg.Collection_site,
				sdg.Universal_id)
			if err != nil {
				log.Fatal(err)
			}
			err = stmt.Close()
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

var persistNautilusVisits = func(db *sql.DB, c chan []NautilusVisit) {

	defer dbSync.Done()

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, visit := range <-c {
		stmt, err := txn.Prepare(`insert into nautilus_visit_staging values($1, $2, $3, $4, $5, $6, $7, $8)`)
		if err != nil {
			log.Fatal(err)
		}
		if stmt != nil {
			_, err = stmt.Exec(
				visit.Visit_id,
				visit.Sample_subject_id,
				visit.Sdg_id,
				visit.Received_on,
				visit.Sd_group_name,
				visit.Sd_visit_name,
				visit.Visit_description,
				visit.Visit_time_date)
			if err != nil {
				log.Fatal(err)
			}
			err = stmt.Close()
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

var persistNautilusAliquots = func(db *sql.DB, c chan []NautilusAliquot) {

	defer dbSync.Done()

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, a := range <-c {
		stmt, err := txn.Prepare(`insert into nautilus_aliquot_staging values(
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
	    $31, $32
        )`)
		if err != nil {
			log.Fatal(err)
		}
		if stmt != nil {
			_, err = stmt.Exec(
				a.SdgId,
				a.SampleSubjectName,
				a.CollectionSite,
				a.PotentialUniversalId,
				a.VisitName,
				a.AliquotId,
				a.AliquotName,
				a.ParentAliquotId,
				a.VisitId,
				a.ReceivedOn,
				a.SampleTypeCode,
				a.SecondarySampleCode,
				a.SampleType,
				a.SecondarySampleType,
				a.FullSampleTypeDesc,
				a.CollectionEventName,
				a.DrawNote,
				a.TissueType,
				a.SpecimenCategory,
				a.CollectMethod,
				a.ReceivedDateTime,
				a.VolumeReceived,
				a.VolumeRemaining,
				a.VolumeUnits,
				a.Concentration,
				a.ConcentrationUnits,
				a.UnitId,
				a.DisposedFlag,
				a.AvailableFlag,
				a.Disposed,
				a.LocationId,
				a.CollectDateTime)
			if err != nil {
				log.Fatal(err)
			}
			err = stmt.Close()
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

var nautilusTransform = func(args []string) {
	target_config := targets[args[0]]

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		target_config.Host,
		target_config.User,
		target_config.Db))

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	stmt, _ := ioutil.ReadFile("transform/NautilusTransform.sql")
	_, err = db.Query(string(stmt))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Nautilus transforms complete.")

}
