package mysqlcmd

import (
	"testing"
)

func Test(t *testing.T) {
	m, err := New("-uroot")
	if err != nil {
		t.Fatal(err)
	}

	lines, err := m.Exec("SELECT 1, 'foo'")
	if err != nil {
		t.Fatal(err)
	}

	if got, expected := len(lines), 1; got != expected {
		t.Fatalf("got %v lines != %v", got, expected)
	}
	if got, expected := string(lines[0][0]), "1"; got != expected {
		t.Fatalf("got %v != %v", got, expected)
	}
	if got, expected := string(lines[0][1]), "foo"; got != expected {
		t.Fatalf("got %v != %v", got, expected)
	}

	err = m.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = m.Err()
	if err != nil {
		t.Fatal(err)
	}
}
