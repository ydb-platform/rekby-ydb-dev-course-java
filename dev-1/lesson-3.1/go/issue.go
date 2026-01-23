package main

import (
	"time"
)

type Issue struct {
	Id        int64     `sql:"id"`
	Title     string    `sql:"title"`
	Timestamp time.Time `sql:"created_at"`
}
