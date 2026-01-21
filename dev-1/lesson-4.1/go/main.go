package main

import (
	"context"
	"log"
	"time"
	"ydb-sample/issue"
	"ydb-sample/schema"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
	connectionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := "grpc://localhost:2136/local"

	db, err := ydb.Open(connectionCtx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(connectionCtx)

	var schemaRepository = schema.NewSchemaRepository(db)
	var issuesRepository = issue.NewIssueRepository(db)

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer queryCancel()

	log.Println("Creating schema...")

	schemaRepository.DropSchema(queryCtx)
	schemaRepository.CreateSchema(queryCtx)

	// ====== INSERT DATA ======
	log.Println("Inserting data...")

	firstIssue, err := issuesRepository.AddIssue(queryCtx, "Ticket 1", "Author 1")
	if err != nil {
		log.Fatalf("Some error happened (1): %+v\n", err)
	}

	secondIssue, err := issuesRepository.AddIssue(queryCtx, "Ticket 2", "Author 2")
	if err != nil {
		log.Fatalf("Some error happened (2): %+v\n", err)
	}

	thirdIssue, err := issuesRepository.AddIssue(queryCtx, "Ticket 3", "Author 3")
	if err != nil {
		log.Fatalf("Some error happened (3): %+v\n", err)
	}

	// ====== CHECK DATA ======
	log.Println("Checking data...")

	allIssues, err := issuesRepository.FindAll(queryCtx)
	if err != nil {
		log.Fatalf("Some error happened while find all: %+v\n", err)
	}

	log.Println("All issues:")
	for _, issue := range allIssues {
		log.Printf("%+v\n", issue)
	}

	first, err := issuesRepository.FindById(queryCtx, firstIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("First: %+v\n", first)
	}

	second, err := issuesRepository.FindById(queryCtx, secondIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Second: %+v\n", second)
	}

	third, err := issuesRepository.FindById(queryCtx, thirdIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Third: %+v\n", second)
	}

	// ====== CHECK TRANSACTIONS ======
	log.Println("Checking non-interactive transaction...")

	result1, err := issuesRepository.LinkTicketsNoInteractive(queryCtx, first.Id, second.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Non-interactive transaction result: %+v\n", result1)
	}

	log.Println("Checking interactive transaction...")

	result2, err := issuesRepository.LinkTicketsInteractive(queryCtx, second.Id, third.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Interactive transaction result: %+v\n", result2)
	}

	// ====== CHECK DATA AGAIN ======
	log.Println("All issues:")

	allIssues, err = issuesRepository.FindAll(queryCtx)
	if err != nil {
		log.Fatalf("Some error happened while find all: %+v\n", err)
	}

	for _, issue := range allIssues {
		log.Printf("%+v\n", issue)
	}
}
