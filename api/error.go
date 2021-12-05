package api

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrNoSuchKey struct {
	Key string
}

func (e ErrNoSuchKey) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("no such key: %s", e.Key))
	msg := fmt.Sprintf("The requested key does not exist in the store: %s", e.Key)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

func (e ErrNoSuchKey) Error() string {
	return e.GRPCStatus().Err().Error()
}
