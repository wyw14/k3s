package audit

import (
	"context"

	"github.com/google/uuid"
)

// operationIDContextKey is an unexported, collision-proof context key type.
type operationIDContextKey struct{}

// NewOperationID returns a fresh, globally unique operation ID.
func NewOperationID() string {
	return uuid.NewString()
}

// WithOperationID returns a copy of ctx that carries the given operation ID.
// An empty operation ID is ignored and the context is returned unchanged.
func WithOperationID(ctx context.Context, operationID string) context.Context {
	if operationID == "" {
		return ctx
	}
	return context.WithValue(ctx, operationIDContextKey{}, operationID)
}

// OperationIDFromContext returns the operation ID carried by ctx, or an empty
// string when ctx does not carry one.
func OperationIDFromContext(ctx context.Context) string {
	operationID, _ := ctx.Value(operationIDContextKey{}).(string)
	return operationID
}

// ensureOperationID returns the operation ID carried by ctx, generating a new
// one when ctx does not carry one.
func ensureOperationID(ctx context.Context) string {
	if operationID := OperationIDFromContext(ctx); operationID != "" {
		return operationID
	}
	return NewOperationID()
}
