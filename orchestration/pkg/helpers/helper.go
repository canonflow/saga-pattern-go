package helpers

import "strings"

func TrimSlice(slice []string) []string {
	data := make([]string, len(slice))

	for _, val := range slice {
		data = append(data, strings.Trim(val, " "))
	}

	return data
}
