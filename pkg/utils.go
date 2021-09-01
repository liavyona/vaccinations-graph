package pkg

import "fmt"

func GroupByKey(entities []map[string]interface{}, key string) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(entities))
	for _, entity := range entities {
		if keyVal, ok := entity[key]; ok {
			result[keyVal.(string)] = entity
		} else {
			return nil, fmt.Errorf("failed to find %s key", key)
		}
	}
	return result, nil
}

func IsStringInlist(items []string, val string) bool {
	if items == nil || len(items) == 0 {
		return false
	}
	for _, item := range items {
		if item == val {
			return true
		}
	}
	return false
}