package database

import (
	"database/sql"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // Postgres driver
)

func Connect() *sql.DB {
	// load .env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN not found in env")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("Failed to open DB:", err)
	}

	// verify connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to connect to DB:", err)
	}

	log.Println("✅ Database connected successfully")
	return db
}
