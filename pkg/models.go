package pkg

import "time"

type VaccinationStatusEntity struct {
	CountryCode string `json:"iso_code"`
	Data        []struct {
		Date                  string `json:"date"`
		TotalPeopleVaccinated int64  `json:"people_vaccinated"`
	}
}

type VaccinationStatus struct {
	CountryCode           string
	UpdatedAt             time.Time
	TotalPeopleVaccinated int64
}

type ApiMetadata struct {
	URL             string
	CountryCodeKey  string
	CountriesFilter []string
}
