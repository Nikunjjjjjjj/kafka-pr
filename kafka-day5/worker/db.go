package worker

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

var DB *sql.DB

func ConnectDB() {
	conn := "postgres://postgres:1234@localhost:5432/gindb?sslmode=disable"

	var err error
	DB, err = sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("Failed to connect DB:", err)
	}

	if err = DB.Ping(); err != nil {
		log.Fatal("Postgres unreachable:", err)
	}

	log.Println("DB Connected!")
}
