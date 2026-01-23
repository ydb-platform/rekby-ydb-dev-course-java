package main

import (
	"context"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// Репозиторий для управления схемой базы данных YDB
// Отвечает за создание и удаление таблиц
type SchemaRepository struct {
	driver *ydb.Driver
}

func NewSchemaRepository(driver *ydb.Driver) *SchemaRepository {
	return &SchemaRepository{
		driver: driver,
	}
}

// Создает таблицу issues в базе данных
// Таблица содержит поля:
// - id: уникальный идентификатор тикета
// - title: название тикета
// - created_at: время создания тикета
// Все поля являются обязательными.
func (repo *SchemaRepository) CreateSchema(ctx context.Context) {
	err := repo.driver.Query().Exec(
		ctx,
		`
		CREATE TABLE IF NOT EXISTS issues (
			id Int64 NOT NULL,
			title Text NOT NULL,
			created_at Timestamp NOT NULL,
			PRIMARY KEY (id)
		);
		`,
		query.WithParameters(ydb.ParamsBuilder().Build()),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// Удаляет таблицу issues из базы данных
// Используется для очистки схемы перед созданием новой
func (repo *SchemaRepository) DropSchema(ctx context.Context) {
	err := repo.driver.Query().Exec(
		ctx,
		"DROP TABLE IF EXISTS issues;",
		query.WithParameters(ydb.ParamsBuilder().Build()),
	)
	if err != nil {
		log.Fatal(err)
	}
}
