package common

import (
	"time"
)

var MAX_SECONDS int = 2


func WaitingTooLong(start time.Time) bool {
	return time.Since(start) > (time.Duration(MAX_SECONDS) * time.Second)
}
