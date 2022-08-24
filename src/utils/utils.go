package utils

import (
	"math"
)

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

func Max(a, b uint64) int {
	if a < b {
		return int(b)
	} else {
		return int(a)
	}
}

func IsDir(path string) bool {
	return path[len(path)-1] == '/'
}
