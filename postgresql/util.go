package postgresql

import "strings"

const EscapeChar = `"`

// EscapeName escapes a database name (table or column) unless it's schema-qualified.
func EscapeName(val string) string {
	if strings.Contains(val, ".") {
		return val
	}
	return EscapeChar + val + EscapeChar
}
