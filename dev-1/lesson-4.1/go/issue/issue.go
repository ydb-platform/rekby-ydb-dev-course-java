package issue

import (
	"time"
)

type Issue struct {
	Id         int64     `sql:"id"`
	Title      string    `sql:"title"`
	Timestamp  time.Time `sql:"created_at"`
	Author     string    `sql:"author"`
	LinksCount uint64    `sql:"links_count"`
}
