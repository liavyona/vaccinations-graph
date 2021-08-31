package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/fatih/structs"
	"github.com/rs/zerolog/log"

	"github.com/liavyona/vaccinations-graph/pkg/pkg"
)

var apiMetadata *pkg.ApiMetadata
var arangoDb *pkg.ArangoDB

func init() {
	apiMetadata = &pkg.ApiMetadata{
		URL:             "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.json",
		CountryCodeKey:  "iso_code",
		CountriesFilter: []string{"ISR", "USA", "FRA", "ESP"},
	}
	arangoEndpoint := os.Getenv("ARANGO_ENDPOINT")
	arangoUsername := os.Getenv("ARANGO_USER_NAME")
	arangoPassword := os.Getenv("ARANGO_PASS")
	arangoCertificate := os.Getenv("ARANGO_CERTIFICATE")
	arangoDatabase := os.Getenv("ARANGO_DATABASE")
	if arangoEndpoint == "" || arangoUsername == "" || arangoPassword == "" || arangoCertificate == "" || arangoDatabase == "" {
		log.Fatal().Msg("ARANGO_ENDPOINT, ARANGO_USER_NAME, ARANGO_PASS, ARANGO_CERTIFICATE AND ARANGO_DATABASE must be provided")
	}
	dbWrapper, err := pkg.ConnectToArango(
		arangoEndpoint,
		arangoUsername,
		arangoPassword,
		arangoCertificate,
		arangoDatabase,
	)
	if err != nil {
		log.Fatal().Str("endpoint", arangoEndpoint).Err(err).Msg("Error while connecting to arango db")
	} else {
		arangoDb = dbWrapper
	}
}

func handleNewRun(
	ctx context.Context,
	currentRun,
	prevRunId string,
) (prevRunNodes map[string]interface{}, err error) {
	hasPrevRun := true
	prevRun, err := arangoDb.GetVaccinationNodes(
		ctx,
		prevRunId,
	)
	if err != nil || len(prevRun) == 0 {
		hasPrevRun = false
	}
	prevRunNodes, err = pkg.GroupByKey(prevRun, "CountryCode")
	if err != nil {
		log.Err(err).Strs("countries", apiMetadata.CountriesFilter).Msg("Unknown format of previous run nodes")
		return prevRunNodes, errors.New("Unknown format of previous run nodes")
	}
	err = arangoDb.CreateNewRunNode(
		ctx,
		currentRun,
	)
	if err != nil {
		log.Err(err).Strs("countries", apiMetadata.CountriesFilter).Msg("Failed to create new run node")
		return prevRunNodes, errors.New("Failed to create new run node")
	}
	if hasPrevRun {
		err = arangoDb.CreateEdgeBetweenRuns(
			ctx,
			currentRun,
			prevRunId,
		)
		if err != nil {
			log.Err(err).Strs("countries", apiMetadata.CountriesFilter).
				Msg("Failed to edge between current run and previous run")
			return prevRunNodes, errors.New("Failed to edge between current run and previous run")
		}
	}
	return prevRunNodes, nil
}

func compareToPrevRunNodes(
	vaccinationsData map[string]pkg.VaccinationStatus,
	currentRun string,
	prevRunNodes map[string]interface{},
) (map[string][]interface{}, error) {
	var vaccinationNodes = map[string][]interface{}{
		"new":     {},
		"old":     {},
		"changed": {},
	}
	for country, data := range vaccinationsData {
		if prevNodeObj, ok := prevRunNodes[country]; ok {
			var prevNode map[string]interface{}
			parsed, err := json.Marshal(prevNodeObj)
			if err != nil {
				log.Err(err).Str("country", country).Msg("Failed to load previous run node")
				return vaccinationNodes, errors.New("failed to load previous run node")
			}
			err = json.Unmarshal(parsed, &prevNode)
			if err != nil {
				log.Err(err).Str("country", country).Msg("Failed to dump previous run node")
				return vaccinationNodes, errors.New("failed to dump previous run node")
			}
			prevNodeVaccinations := prevNode["TotalPeopleVaccinated"].(float64)
			if float64(data.TotalPeopleVaccinated) == prevNodeVaccinations {
				// We need only the id of the previous node since we won't create a new Vaccination document
				vaccinationNodes["old"] = append(vaccinationNodes["old"], prevNode["_id"])
				log.Debug().Str("country", country).Int64("vaccinations", data.TotalPeopleVaccinated).
					Msg("No changed since in total vaccinations since previous run")
			} else {
				node := structs.Map(data)
				node["date"] = currentRun
				node["_key"] = fmt.Sprintf("%s-%s", currentRun, data.CountryCode)
				node["collection"] = "Vaccinations"
				node["diff"] = float64(data.TotalPeopleVaccinated) - prevNodeVaccinations
				node["prevAssetId"] = prevNode["_id"]
				vaccinationNodes["changed"] = append(vaccinationNodes["changed"], node)
				log.Debug().Str("country", country).Int64("vaccinations", data.TotalPeopleVaccinated).
					Float64("previous", prevNodeVaccinations).
					Msg("Total vaccinations changed since previous run")
			}
		} else {
			// New country so we create a new node
			node := structs.Map(data)
			node["date"] = currentRun
			node["_key"] = fmt.Sprintf("%s-%s", currentRun, data.CountryCode)
			node["collection"] = "Vaccinations"
			vaccinationNodes["new"] = append(vaccinationNodes["new"], node)
			log.Debug().Str("country", country).Int64("vaccinations", data.TotalPeopleVaccinated).
				Msg("First run for country")
		}
	}
	return vaccinationNodes, nil
}

func saveVaccinationsData(ctx context.Context) error {
	vaccinationsData, err := apiMetadata.GetVaccinationStatus()
	if err != nil {
		log.Err(err).Strs("countries", apiMetadata.CountriesFilter).Msg("Failed to get vaccinations status")
		return errors.New("failed to get vaccinations status")
	}
	currentTime := time.Now()
	currentRun := currentTime.Format("2006-01-02")
	prevRunId := currentTime.AddDate(0, 0, -1).Format("2006-01-02")
	prevRunNodes, err := handleNewRun(
		ctx,
		currentRun,
		prevRunId,
	)
	if err != nil {
		log.Err(err).Str("current_run", currentRun).Msg("Failed to setup current run")
		return errors.New("failed to setup current run")
	}
	vaccinationNodes, err := compareToPrevRunNodes(
		vaccinationsData,
		currentRun,
		prevRunNodes,
	)
	if err != nil {
		log.Err(err).Str("current_run", currentRun).Msg("Failed to compare current run with previous run")
		return errors.New("failed to compare current run with previous run")
	}
	err = arangoDb.HandleNewCountries(ctx,
		&log.Logger,
		currentRun,
		vaccinationNodes["new"])
	if err != nil {
		log.Err(err).Int("nodes", len(vaccinationNodes["new"])).
			Msg("Failed to create nodes for new countries")
	} else {
		log.Info().Int("nodes", len(vaccinationNodes["new"])).
			Msg("Successfully created nodes for new countries")
	}
	err = arangoDb.HandleNewEdges(
		ctx,
		&log.Logger,
		currentRun,
		vaccinationNodes["old"])
	if err != nil {
		log.Err(err).Int("edges", len(vaccinationNodes["old"])).
			Msg("Failed to create edges for existing countries")
	} else {
		log.Info().Int("edges", len(vaccinationNodes["old"])).
			Msg("Successfully created edges for existing countries")
	}
	if len(vaccinationNodes["changed"]) > 0 {
		err = arangoDb.HandleChangedCountries(
			ctx,
			&log.Logger,
			currentRun,
			vaccinationNodes["changed"])
		if err != nil {
			log.Err(err).Int("nodes", len(vaccinationNodes["changed"])).
				Msg("Failed to create edges for existing countries")
			return err
		}
	}
	log.Info().Int("nodes", len(vaccinationNodes["changed"])).
		Msg("Successfully created nodes for existing countries")
	return nil
}

func main() {
	lambda.Start(saveVaccinationsData)
}
