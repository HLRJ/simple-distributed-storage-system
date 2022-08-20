package utils

import (
	"errors"
	"math"
	"math/rand"
	"time"
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

func RandomChooseLocs(locs []int, count int) ([]int, error) {
	if len(locs) < count {
		return nil, errors.New("insufficient locs")
	}

	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(locs), func(i int, j int) {
		locs[i], locs[j] = locs[j], locs[i]
	})

	result := make([]int, 0, count)
	for index, value := range locs {
		if index == count {
			break
		}
		result = append(result, value)
	}
	return result, nil
}
