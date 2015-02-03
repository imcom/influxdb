package influxql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure a value's data type can be retrieved.
func TestInspectDataType(t *testing.T) {
	for i, tt := range []struct {
		v   interface{}
		typ influxql.DataType
	}{
		{float64(100), influxql.Number},
	} {
		if typ := influxql.InspectDataType(tt.v); tt.typ != typ {
			t.Errorf("%d. %v (%s): unexpected type: %s", i, tt.v, tt.typ, typ)
			continue
		}
	}
}

// Ensure the SELECT statement can extract substatements.
func TestSelectStatement_Substatement(t *testing.T) {
	var tests = []struct {
		stmt string
		expr *influxql.VarRef
		sub  string
		err  string
	}{
		// 0. Single series
		{
			stmt: `SELECT value FROM myseries WHERE value > 1`,
			expr: &influxql.VarRef{Val: "value"},
			sub:  `SELECT value FROM myseries WHERE value > 1.000`,
		},

		// 1. Simple join
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM join(aa,bb)`,
			expr: &influxql.VarRef{Val: "aa.value"},
			sub:  `SELECT aa.value FROM aa`,
		},

		// 2. Simple merge
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM merge(aa, bb)`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb`,
		},

		// 3. Join with condition
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM join(aa, bb) WHERE aa.host = 'servera' AND bb.host = 'serverb'`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb WHERE bb.host = 'serverb'`,
		},

		// 4. Join with complex condition
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM join(aa, bb) WHERE aa.host = 'servera' AND (bb.host = 'serverb' OR bb.host = 'serverc') AND 1 = 2`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb WHERE (bb.host = 'serverb' OR bb.host = 'serverc') AND 1.000 = 2.000`,
		},

		// 5. 4 with different condition order
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM join(aa, bb) WHERE ((bb.host = 'serverb' OR bb.host = 'serverc') AND aa.host = 'servera') AND 1 = 2`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb WHERE ((bb.host = 'serverb' OR bb.host = 'serverc')) AND 1.000 = 2.000`,
		},
	}

	for i, tt := range tests {
		// Parse statement.
		stmt, err := influxql.NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}

		// Extract substatement.
		sub, err := stmt.(*influxql.SelectStatement).Substatement(tt.expr)
		if err != nil {
			t.Errorf("%d. %q: unexpected error: %s", i, tt.stmt, err)
			continue
		}
		if substr := sub.String(); tt.sub != substr {
			t.Errorf("%d. %q: unexpected substatement:\n\nexp=%s\n\ngot=%s\n\n", i, tt.stmt, tt.sub, substr)
			continue
		}
	}
}

// Ensure the time range of an expression can be extracted.
func TestTimeRange(t *testing.T) {
	for i, tt := range []struct {
		expr     string
		min, max string
	}{
		// LHS VarRef
		{expr: `time > '2000-01-01 00:00:00'`, min: `2000-01-01 00:00:00.000001`, max: `0001-01-01 00:00:00`},
		{expr: `time >= '2000-01-01 00:00:00'`, min: `2000-01-01 00:00:00`, max: `0001-01-01 00:00:00`},
		{expr: `time < '2000-01-01 00:00:00'`, min: `0001-01-01 00:00:00`, max: `1999-12-31 23:59:59.999999`},
		{expr: `time <= '2000-01-01 00:00:00'`, min: `0001-01-01 00:00:00`, max: `2000-01-01 00:00:00`},

		// RHS VarRef
		{expr: `'2000-01-01 00:00:00' > time`, min: `0001-01-01 00:00:00`, max: `1999-12-31 23:59:59.999999`},
		{expr: `'2000-01-01 00:00:00' >= time`, min: `0001-01-01 00:00:00`, max: `2000-01-01 00:00:00`},
		{expr: `'2000-01-01 00:00:00' < time`, min: `2000-01-01 00:00:00.000001`, max: `0001-01-01 00:00:00`},
		{expr: `'2000-01-01 00:00:00' <= time`, min: `2000-01-01 00:00:00`, max: `0001-01-01 00:00:00`},

		// Equality
		{expr: `time = '2000-01-01 00:00:00'`, min: `2000-01-01 00:00:00`, max: `2000-01-01 00:00:00`},

		// Multiple time expressions.
		{expr: `time >= '2000-01-01 00:00:00' AND time < '2000-01-02 00:00:00'`, min: `2000-01-01 00:00:00`, max: `2000-01-01 23:59:59.999999`},

		// Min/max crossover
		{expr: `time >= '2000-01-01 00:00:00' AND time <= '1999-01-01 00:00:00'`, min: `2000-01-01 00:00:00`, max: `1999-01-01 00:00:00`},

		// Absolute time
		{expr: `time = 1388534400s`, min: `2014-01-01 00:00:00`, max: `2014-01-01 00:00:00`},

		// Non-comparative expressions.
		{expr: `time`, min: `0001-01-01 00:00:00`, max: `0001-01-01 00:00:00`},
		{expr: `time + 2`, min: `0001-01-01 00:00:00`, max: `0001-01-01 00:00:00`},
		{expr: `time - '2000-01-01 00:00:00'`, min: `0001-01-01 00:00:00`, max: `0001-01-01 00:00:00`},
		{expr: `time AND '2000-01-01 00:00:00'`, min: `0001-01-01 00:00:00`, max: `0001-01-01 00:00:00`},
	} {
		// Extract time range.
		expr := MustParseExpr(tt.expr)
		min, max := influxql.TimeRange(expr)

		// Compare with expected min/max.
		if min := min.Format(influxql.DateTimeFormat); tt.min != min {
			t.Errorf("%d. %s: unexpected min:\n\nexp=%s\n\ngot=%s\n\n", i, tt.expr, tt.min, min)
			continue
		}
		if max := max.Format(influxql.DateTimeFormat); tt.max != max {
			t.Errorf("%d. %s: unexpected max:\n\nexp=%s\n\ngot=%s\n\n", i, tt.expr, tt.max, max)
			continue
		}
	}
}

// Ensure an AST node can be rewritten.
func TestRewrite(t *testing.T) {
	expr := MustParseExpr(`time > 1 OR foo = 2`)

	// Flip LHS & RHS in all binary expressions.
	act := influxql.RewriteFunc(expr, func(n influxql.Node) influxql.Node {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			return &influxql.BinaryExpr{Op: n.Op, LHS: n.RHS, RHS: n.LHS}
		default:
			return n
		}
	})

	// Verify that everything is flipped.
	if act := act.String(); act != `2.000 = foo OR 1.000 > time` {
		t.Fatalf("unexpected result: %s", act)
	}
}

// Ensure an expression can be reduced.
func TestEval(t *testing.T) {
	for i, tt := range []struct {
		in   string
		out  interface{}
		data map[string]interface{}
	}{
		// Number literals.
		{in: `1 + 2`, out: float64(3)},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: float64(26.5), data: map[string]interface{}{"foo": float64(5)}},
		{in: `foo / 2`, out: float64(2), data: map[string]interface{}{"foo": float64(4)}},
		{in: `4 = 4`, out: true},
		{in: `4 <> 4`, out: false},
		{in: `6 > 4`, out: true},
		{in: `4 >= 4`, out: true},
		{in: `4 < 6`, out: true},
		{in: `4 <= 4`, out: true},
		{in: `4 AND 5`, out: nil},

		// Boolean literals.
		{in: `true AND false`, out: false},
		{in: `true OR false`, out: true},

		// String literals.
		{in: `'foo' = 'bar'`, out: false},
		{in: `'foo' = 'foo'`, out: true},

		// Variable references.
		{in: `foo`, out: "bar", data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: true, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: nil, data: map[string]interface{}{"foo": nil}},
		{in: `foo <> 'bar'`, out: true, data: map[string]interface{}{"foo": "xxx"}},
	} {
		// Evaluate expression.
		out := influxql.Eval(MustParseExpr(tt.in), tt.data)

		// Compare with expected output.
		if !reflect.DeepEqual(tt.out, out) {
			t.Errorf("%d. %s: unexpected output:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}

// Ensure an expression can be reduced.
func TestReduce(t *testing.T) {
	now := mustParseTime("2000-01-01T00:00:00Z")

	for i, tt := range []struct {
		in   string
		out  string
		data Valuer
	}{
		// Number literals.
		{in: `1 + 2`, out: `3.000`},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: `(foo * 2.000) + 16.500`},
		{in: `foo(bar(2 + 3), 4)`, out: `foo(bar(5.000), 4.000)`},
		{in: `4 / 0`, out: `0.000`},
		{in: `4 = 4`, out: `true`},
		{in: `4 <> 4`, out: `false`},
		{in: `6 > 4`, out: `true`},
		{in: `4 >= 4`, out: `true`},
		{in: `4 < 6`, out: `true`},
		{in: `4 <= 4`, out: `true`},
		{in: `4 AND 5`, out: `4.000 AND 5.000`},

		// Boolean literals.
		{in: `true AND false`, out: `false`},
		{in: `true OR false`, out: `true`},
		{in: `true OR (foo = bar AND 1 > 2)`, out: `true`},
		{in: `(foo = bar AND 1 > 2) OR true`, out: `true`},
		{in: `false OR (foo = bar AND 1 > 2)`, out: `false`},
		{in: `(foo = bar AND 1 > 2) OR false`, out: `false`},
		{in: `true = false`, out: `false`},
		{in: `true <> false`, out: `true`},
		{in: `true + false`, out: `true + false`},

		// Time literals.
		{in: `now() + 2h`, out: `"2000-01-01 02:00:00"`, data: map[string]interface{}{"now()": now}},
		{in: `now() / 2h`, out: `"2000-01-01 00:00:00" / 2h`, data: map[string]interface{}{"now()": now}},
		{in: `4µ + now()`, out: `"2000-01-01 00:00:00.000004"`, data: map[string]interface{}{"now()": now}},
		{in: `now() = now()`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() <> now()`, out: `false`, data: map[string]interface{}{"now()": now}},
		{in: `now() < now() + 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() <= now() + 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() >= now() - 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() > now() - 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() - (now() - 60s)`, out: `1m`, data: map[string]interface{}{"now()": now}},
		{in: `now() AND now()`, out: `"2000-01-01 00:00:00" AND "2000-01-01 00:00:00"`, data: map[string]interface{}{"now()": now}},
		{in: `now()`, out: `now()`},

		// Duration literals.
		{in: `10m + 1h - 60s`, out: `69m`},
		{in: `(10m / 2) * 5`, out: `25m`},
		{in: `60s = 1m`, out: `true`},
		{in: `60s <> 1m`, out: `false`},
		{in: `60s < 1h`, out: `true`},
		{in: `60s <= 1h`, out: `true`},
		{in: `60s > 12s`, out: `true`},
		{in: `60s >= 1m`, out: `true`},
		{in: `60s AND 1m`, out: `1m AND 1m`},
		{in: `60m / 0`, out: `0s`},
		{in: `60m + 50`, out: `1h + 50.000`},

		// String literals.
		{in: `'foo' + 'bar'`, out: `'foobar'`},

		// Variable references.
		{in: `foo`, out: `'bar'`, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: `true`, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: `false`, data: map[string]interface{}{"foo": nil}},
		{in: `foo <> 'bar'`, out: `false`, data: map[string]interface{}{"foo": nil}},
	} {
		// Fold expression.
		expr := influxql.Reduce(MustParseExpr(tt.in), tt.data)

		// Compare with expected output.
		if out := expr.String(); tt.out != out {
			t.Errorf("%d. %s: unexpected expr:\n\nexp=%s\n\ngot=%s\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}

// Valuer represents a simple wrapper around a map to implement the influxql.Valuer interface.
type Valuer map[string]interface{}

// Value returns the value and existence of a key.
func (o Valuer) Value(key string) (v interface{}, ok bool) {
	v, ok = o[key]
	return
}
