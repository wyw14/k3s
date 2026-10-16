package audit

import (
	"fmt"
	"reflect"
	"strings"
)

// RedactedMask is the replacement value used for redacted attributes when no
// custom mask has been configured.
const RedactedMask = "***"

// DefaultSensitiveKeys is the default set of (lower-cased) attribute key
// fragments that are considered sensitive. A key is sensitive when it equals,
// or contains, any configured fragment, so e.g. "client_token" matches the
// "token" fragment.
var DefaultSensitiveKeys = []string{
	"password",
	"passwd",
	"secret",
	"token",
	"credential",
	"credentials",
	"api_key",
	"apikey",
	"access_key",
	"accesskey",
	"private_key",
}

// Redactor replaces values of sensitive attributes with a fixed mask. It is
// safe for concurrent use after construction.
type Redactor struct {
	mask      string
	sensitive map[string]struct{}
}

// NewRedactor builds a Redactor that replaces sensitive values with mask.
// keys lists lower-cased key fragments; an attribute key is redacted when it
// equals or contains one of them. An empty mask falls back to RedactedMask and
// an empty key list installs DefaultSensitiveKeys.
func NewRedactor(mask string, keys []string) *Redactor {
	if mask == "" {
		mask = RedactedMask
	}
	if len(keys) == 0 {
		keys = DefaultSensitiveKeys
	}
	r := &Redactor{
		mask:      mask,
		sensitive: make(map[string]struct{}, len(keys)),
	}
	for _, key := range keys {
		if key = strings.ToLower(strings.TrimSpace(key)); key != "" {
			r.sensitive[key] = struct{}{}
		}
	}
	return r
}

// DefaultRedactor returns a Redactor configured with DefaultSensitiveKeys and
// RedactedMask.
func DefaultRedactor() *Redactor {
	return NewRedactor(RedactedMask, DefaultSensitiveKeys)
}

// Mask reports the configured replacement for sensitive values.
func (r *Redactor) Mask() string {
	return r.mask
}

// IsSensitive reports whether the given attribute key is considered sensitive
// by the redactor configuration.
func (r *Redactor) IsSensitive(key string) bool {
	name := strings.ToLower(key)
	for fragment := range r.sensitive {
		if name == fragment || strings.Contains(name, fragment) {
			return true
		}
	}
	return false
}

// Redact returns a deep copy of value in which every sensitive attribute is
// replaced with the configured mask. The original value is never modified.
// Maps (including typed maps such as map[string]string), slices and arrays are
// walked recursively; other values are copied verbatim.
func (r *Redactor) Redact(value any) any {
	if value == nil {
		return nil
	}
	return r.redactValue(reflect.ValueOf(value))
}

func (r *Redactor) redactValue(value reflect.Value) any {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return r.redactValue(value.Elem())
	case reflect.Map:
		return r.redactMap(value)
	case reflect.Slice:
		if value.IsNil() {
			return nil
		}
		fallthrough
	case reflect.Array:
		redacted := make([]any, value.Len())
		for i := range redacted {
			redacted[i] = r.redactValue(value.Index(i))
		}
		return redacted
	default:
		if !value.IsValid() {
			return nil
		}
		return value.Interface()
	}
}

func (r *Redactor) redactMap(value reflect.Value) map[string]any {
	if value.IsNil() {
		return nil
	}
	redacted := make(map[string]any, value.Len())
	for _, key := range value.MapKeys() {
		name := stringerKey(key)
		if name == "" || r.IsSensitive(name) {
			redacted[name] = r.mask
			continue
		}
		redacted[name] = r.redactValue(value.MapIndex(key))
	}
	return redacted
}

// stringerKey renders a reflect map key as a string. Audit attribute maps are
// expected to use string keys; non-string keys are coerced with fmt.Sprint.
func stringerKey(key reflect.Value) string {
	if key.Kind() == reflect.String {
		return key.String()
	}
	return fmt.Sprint(key.Interface())
}
