package main

import (
	"context"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// author: Egor Danilov
func main() {
	connectionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := "grpc://localhost:2136/local"

	db, err := ydb.Open(connectionCtx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(connectionCtx)

	schemaRepository := NewSchemaRepository(db)
	issuesRepository := NewIssueRepository(db)

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer queryCancel()

	schemaRepository.DropSchema(queryCtx)
	schemaRepository.CreateSchema(queryCtx)

	firstIssue, err := issuesRepository.AddIssue(queryCtx, "Ticket 1")
	if err != nil {
		log.Fatalf("Some error happened (1): %v\n", err)
	}

	_, err = issuesRepository.AddIssue(queryCtx, "Ticket 2")
	if err != nil {
		log.Fatalf("Some error happened (2): %v\n", err)
	}

	_, err = issuesRepository.AddIssue(queryCtx, "Ticket 3")
	if err != nil {
		log.Fatalf("Some error happened (3): %v\n", err)
	}

	issues, err := issuesRepository.FindAll(queryCtx)
	if err != nil {
		log.Fatalf("Some error happened while finding all: %v\n", err)
	}
	for _, issue := range issues {
		log.Printf("Issue: %v\n", issue)
	}

	foundFirstIssue, err := issuesRepository.FindById(queryCtx, firstIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("First issue: %v\n", foundFirstIssue)
	}
}
