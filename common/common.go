package common

import (
	"time"
)

var MAX_SECONDS int = 2
var POLL_INTERVAL = 10 * time.Millisecond


func WaitingTooLong(start time.Time) bool {
	return time.Since(start) > (time.Duration(MAX_SECONDS) * time.Second)
}
