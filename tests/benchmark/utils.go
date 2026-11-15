package benchmark

import "strconv"

func createKeyList(keyCount int) []string {
	res := []string{}

	for i := range keyCount {
		res = append(res, strconv.Itoa(i))
	}

	return res
}