package utils

import "math"

func CeilDiv(a, b uint64) int {
	return int(math.Ceil(float64(a) / float64(b)))
}

func Min(a, b uint64) int {
	if a < b {
		return int(a)
	} else {
		return int(b)
	}
}