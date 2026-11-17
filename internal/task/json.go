package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
)

// EqualJSON compares exact JSON values without rounding numbers through float64.
// Absent parameters retain the existing empty-object default. Duplicate object
// keys are rejected so different consumers cannot disagree about the input.
func EqualJSON(a, b []byte) (bool, error) {
	av, err := jsonValue(a)
	if err != nil {
		return false, err
	}
	bv, err := jsonValue(b)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(av, bv), nil
}
func jsonValue(raw []byte) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readValue(d, 0)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("params must contain one JSON value")
	}
	return v, nil
}
func readValue(d *json.Decoder, depth int) (any, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := token.(type) {
	case json.Delim:
		if depth >= 256 {
			return nil, fmt.Errorf("JSON nesting exceeds 256 containers")
		}
		switch v {
		case '{':
			m := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				k, ok := key.(string)
				if !ok {
					return nil, fmt.Errorf("invalid object key")
				}
				if _, exists := m[k]; exists {
					return nil, fmt.Errorf("duplicate JSON key %q", k)
				}
				value, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				m[k] = value
			}
			if _, err := d.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			values := []any{}
			for d.More() {
				value, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			if _, err := d.Token(); err != nil {
				return nil, err
			}
			return values, nil
		}
		return nil, fmt.Errorf("unexpected JSON delimiter")
	case json.Number:
		return exactNumber(v.String()), nil
	default:
		return v, nil
	}
}

// Keep the exponent symbolic. A small input such as 1e999999999 must never
// allocate an integer with that many digits merely to compare two parameters.
type numberValue string

func exactNumber(s string) numberValue {
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	exp := new(big.Int)
	if at := strings.IndexAny(s, "eE"); at >= 0 {
		exp.SetString(s[at+1:], 10)
		s = s[:at]
	}
	if at := strings.IndexByte(s, '.'); at >= 0 {
		exp.Sub(exp, big.NewInt(int64(len(s)-at-1)))
		s = s[:at] + s[at+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	trimmed := strings.TrimRight(s, "0")
	exp.Add(exp, big.NewInt(int64(len(s)-len(trimmed))))
	return numberValue(sign + trimmed + "e" + exp.String())
}
