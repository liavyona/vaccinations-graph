package pkg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (api *ApiMetadata) GetVaccinationStatus() (vaccinationsStatus map[string]VaccinationStatus, err error) {
	var globalVaccinationsStatus []map[string]interface{}
	resp, err := http.Get(api.URL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // nolint: errcheck
	err = json.NewDecoder(resp.Body).Decode(&globalVaccinationsStatus)
	if err != nil {
		return nil, err
	}
	vaccinationsStatusMap, err := GroupByKey(globalVaccinationsStatus, api.CountryCodeKey)
	if err != nil {
		return nil, err
	}
	if len(api.CountriesFilter) > 0 {
		vaccinationsStatus = make(map[string]VaccinationStatus, len(api.CountriesFilter))
	} else {
		vaccinationsStatus = make(map[string]VaccinationStatus, len(vaccinationsStatusMap))
	}
	for countryCode, entity := range vaccinationsStatusMap {
		if len(api.CountriesFilter) == 0 || IsStringInlist(api.CountriesFilter, countryCode) {
			parsed, err := json.Marshal(entity)
			if err != nil {
				return nil, err
			}
			var status VaccinationStatusEntity
			err = json.Unmarshal(parsed, &status)
			if err != nil {
				return nil, err
			}
			if len(status.Data) == 0 {
				return nil, fmt.Errorf("no data available for country: %s", countryCode)
			}
			lastStatus := status.Data[len(status.Data)-1]
			lastStatusDate, err := time.Parse("2006-01-02", lastStatus.Date)
			if err != nil {
				return nil, err
			}
			vaccinationsStatus[countryCode] = VaccinationStatus{CountryCode: countryCode, UpdatedAt: lastStatusDate, TotalPeopleVaccinated: lastStatus.TotalPeopleVaccinated}

		}
	}
	return vaccinationsStatus, nil
}
