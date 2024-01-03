package postgresql

import (
	"reflect"
	"strings"
)

const EscapeChar = `"`

// EscapeName escapes a database name (table or column) unless it's schema-qualified.
func EscapeName(val string) string {
	if strings.Contains(val, ".") {
		return val
	}
	return EscapeChar + val + EscapeChar
}

type (
	PlaceHolderFunc   func(index int) string
	HelperFunc[T any] func(values T) ([]string, []string, []interface{})
)

func CreatePreparedStatementHelper[T any](placeholder PlaceHolderFunc) HelperFunc[T] {
	return func(values T) ([]string, []string, []interface{}) {
		v := reflect.ValueOf(values)
		t := v.Type()

		var fields []string
		var placeholders []string
		var args []interface{}

		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("db")
			if tag == "" || tag == "-" || len(tag) == 0 {
				continue
			}
			fields = append(fields, EscapeName(tag))
			placeholders = append(placeholders, placeholder(i))
			args = append(args, v.Field(i).Interface())
		}

		return fields, placeholders, args
	}
}

func GetSetArgs(fields []string, placeholders []string) string {
	var setArgs []string
	for i, field := range fields {
		setArg := field + " = " + placeholders[i]
		setArgs = append(setArgs, setArg)
	}

	return strings.Join(setArgs, ", ")
}
