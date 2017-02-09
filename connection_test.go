// +build integration

package gohive

import "testing"

/*
Used with local testing: expects hiveserver2 running on 127.0.0.1:10000,
with a single table defined, "foo".
*/

func TestShowTables(t *testing.T) {
	var (
		ct        int = 0
		tableName string
	)
	conn, err := Connect("114.215.255.134:10000", DefaultOptions)
	if err != nil {
		t.Fatalf("Connect error %v", err)
	}

	rows, err := conn.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("Connection.Query error: %v", err)
	}

	status, err := rows.Wait()
	if err != nil {
		t.Fatalf("Connection.Wait error: %v", err)
	}

	if !status.IsSuccess() {
		t.Fatalf("Unsuccessful query execution: %v", status)
	}

	for rows.Next() {
		if ct > 0 {
			t.Fatal("Rows.Next should terminate after 1 fetch")
		}
		rows.Scan(&tableName)
		ct++
	}

	if tableName != "foo" {
		t.Errorf("Expected table 'foo' but found %s", tableName)
	}
}

//func TestQuery(t *testing.T) {
//	var (
//		ct    int = 0
//		id    int
//		value string
//	)

//	db, err := Connect("114.215.255.134:10000", DefaultOptions)
//	rows, err := db.Query("select * from foo")
//	if err != nil {
//		t.Fatalf("Connect error: %v", err)
//	}

//	status, err := rows.Wait()
//	if !status.IsSuccess() {
//		t.Fatalf("Unsuccessful query execution: %v", status)
//	}

//	col := rows.Columns()
//	if !reflect.DeepEqual(col, []string{"foo.id", "foo.val"}) {
//		t.Fatalf("Expected 'id' and 'value' columns, but got %v", col)
//	}

//	vals := make([]string, 0)
//	for rows.Next() {
//		ct++
//		rows.Scan(&id, &value)

//		if id != ct {
//			t.Errorf("Expected row id to be %d but was %d", ct, id)
//		}

//		vals = append(vals, value)
//	}

//	if !reflect.DeepEqual(vals, []string{"foo", "bar", "baz"}) {
//		t.Errorf("Expected 3 row values to be [foo, bar, baz] but was %v", vals)
//	}
//}

//func TestReattach(t *testing.T) {
//	var (
//		ct    int = 0
//		id    int
//		value string
//	)

//	db, err := Connect("114.215.255.134:10000", DefaultOptions)
//	oldRows, err := db.Query("select * from foo")
//	if err != nil {
//		t.Fatalf("Connect error: %v", err)
//	}

//	handle, err := oldRows.Handle()
//	if err != nil {
//		t.Fatalf("Can't read handle: %v", err)
//	}

//	// Reattach.
//	rows, err := Reattach(db, handle)
//	if err != nil {
//		t.Fatalf("Can't reattach: %v", err)
//	}

//	status, err := rows.Wait()
//	if !status.IsSuccess() {
//		t.Fatalf("Unsuccessful query execution: %v", status)
//	}

//	col := rows.Columns()
//	if !reflect.DeepEqual(col, []string{"foo.id", "foo.val"}) {
//		t.Fatalf("Expected 'id' and 'value' columns, but got %v", col)
//	}

//	vals := make([]string, 0)
//	for rows.Next() {
//		ct++
//		rows.Scan(&id, &value)

//		if id != ct {
//			t.Errorf("Expected row id to be %d but was %d", ct, id)
//		}

//		vals = append(vals, value)
//	}

//	if !reflect.DeepEqual(vals, []string{"foo", "bar", "baz"}) {
//		t.Errorf("Expected 3 row values to be [foo, bar, baz] but was %v", vals)
//	}
//}
