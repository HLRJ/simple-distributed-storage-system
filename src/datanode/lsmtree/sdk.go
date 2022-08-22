package lsmtree

import "fmt"

func NewLsmtree(dbDir string, MemTableThresholdNum, SparseKeyDistanceNum, DiskTableNumThresholdNum int) *LSMTree {
	tree, err := Open(dbDir, MemTableThreshold(MemTableThresholdNum), SparseKeyDistance(SparseKeyDistanceNum), DiskTableNumThreshold(DiskTableNumThresholdNum))
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}
	return tree
}
