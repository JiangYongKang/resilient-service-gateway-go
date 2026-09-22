package gateway

import "context"

func contextDeadlineExceeded() error { return context.DeadlineExceeded }
