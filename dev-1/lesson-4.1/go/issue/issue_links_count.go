package issue

type IssueLinksCount struct {
	Id         int64  `sql:"id"`
	LinksCount uint64 `sql:"links_count"`
}
