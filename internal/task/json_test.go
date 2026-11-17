package task

import (
	"strings"
	"testing"
)

func TestEqualJSONExactNumbers(t *testing.T) {
	for _, tc := range []struct {
		a, b  string
		equal bool
	}{
		{`{"n":9007199254740992}`, `{"n":9007199254740993}`, false},
		{`{"a":[1e3,-0,0.01]}`, `{"a":[1000.0,0,1e-2]}`, true},
		{`1e9999999999999999999999999`, `10e9999999999999999999999998`, true},
		{`1e9999999999999999999999999`, `1e9999999999999999999999998`, false},
		{`"1"`, `1`, false}, {`[]`, `{}`, false}, {``, `{}`, true},
	} {
		eq, err := EqualJSON([]byte(tc.a), []byte(tc.b))
		if err != nil || eq != tc.equal {
			t.Fatalf("%s vs %s want %v got %v %v", tc.a, tc.b, tc.equal, eq, err)
		}
	}
}
func TestEqualJSONRejectsAmbiguousOrTrailingInput(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{"a":[{"b":1,"b":2}]}`, `{} {}`, `[}`, `1e+`} {
		if _, err := EqualJSON([]byte(raw), []byte(raw)); err == nil {
			t.Fatalf("must reject %s", raw)
		}
	}
}

func TestEqualJSONDepthBudget(t *testing.T) {
	for _, depth := range []int{256, 257} {
		raw := []byte(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		eq, err := EqualJSON(raw, raw)
		if depth == 256 && (err != nil || !eq) {
			t.Fatalf("budget boundary must be valid: %v %v", eq, err)
		}
		if depth == 257 && err == nil {
			t.Fatal("over-budget JSON must be rejected before recursion grows")
		}
	}
}
