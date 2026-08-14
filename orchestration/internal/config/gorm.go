package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDatabase() *gorm.DB {
	username := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	database := os.Getenv("DB_NAME")

	idleConnectionStr := os.Getenv("DB_IDLE")
	maxConnectionStr := os.Getenv("DB_MAX")
	maxLifeTimeConnectionStr := os.Getenv("DB_LIFETIME")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", username, password, host, port, database)

	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}

	connection, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}

	idleConnection, err := strconv.Atoi(idleConnectionStr)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	maxConnection, err := strconv.Atoi(maxConnectionStr)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	maxLifeTimeConnection, err := strconv.Atoi(maxLifeTimeConnectionStr)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	connection.SetMaxIdleConns(idleConnection)
	connection.SetMaxOpenConns(maxConnection)
	connection.SetConnMaxLifetime(time.Second * time.Duration(maxLifeTimeConnection))

	return db
}
