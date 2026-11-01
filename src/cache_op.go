package paxocache

type CacheOpType uint8

const (
	CacheOpGet = iota
	CacheOpSet
)

// must export fields to include in JSON!
type CacheOp struct {
	Op CacheOpType
	Data string
}

func processCacheOp(op CacheOp, prev string) string {
	switch op.Op {
	case CacheOpGet:
		return prev
	case CacheOpSet:
		return op.Data
	}

	return prev
}