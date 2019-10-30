package gql

import "context"

// CheckCancellation checks if ctx has been cancelled and panics if so.
func CheckCancellation(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		panic("Cancelled: " + err.Error())
	}
}
