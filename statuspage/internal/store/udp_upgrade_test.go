package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestUpgradePreUDPDatabasePreservesTCPRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE current(sid TEXT PRIMARY KEY,name TEXT,online BIGINT,latency BIGINT,ts BIGINT,seq BIGINT)`,
		`CREATE TABLE daily(day TEXT,sid TEXT,up BIGINT DEFAULT 0,down BIGINT DEFAULT 0,lat_sum BIGINT DEFAULT 0,lat_cnt BIGINT DEFAULT 0,PRIMARY KEY(day,sid))`,
		`INSERT INTO current(sid,name,online,latency,ts,seq) VALUES('legacy-sid','Legacy TCP',1,42,1700000000,1)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	_ = db.Close()

	st, err := Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.CurrentRows()
	if err != nil || len(rows) != 1 || rows[0].SID != "legacy-sid" || rows[0].Latency != 42 {
		t.Fatalf("legacy TCP row was not preserved: rows=%+v err=%v", rows, err)
	}
	var count int
	if err := st.queryRow(`SELECT count(*) FROM udp_receivers`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("UDP migration missing or unexpectedly populated: count=%d err=%v", count, err)
	}
}
