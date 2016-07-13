package voltdbclient

import "math"

const (
	PING_HANDLE       = math.MaxInt64
	ASYNC_TOPO_HANDLE = PING_HANDLE - 1
)

const (
	PARTITIONID_BITS = 14

	// maximum values for the txn id fields
	PARTITIONID_MAX_VALUE = (1 << PARTITIONID_BITS) - 1
	MP_INIT_PID           = PARTITIONID_MAX_VALUE
)
