package schema

import (
	"context"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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

// Создает таблицы issues и links в базе данных
//
// Таблица issues содержит поля:
// - id: уникальный идентификатор тикета
// - title: название тикета
// - created_at: время создания тикета
// - author: автор тикета
// - links_count: количество тикетов, привязанных к этому
//
// Таблица links содержит поля source и destination - тикеты,
// между которыми устанавливается связь.
func (repo *SchemaRepository) CreateSchema(ctx context.Context) {
	err := repo.driver.Query().Exec(
		ctx,
		`
		CREATE TABLE IF NOT EXISTS issues (
			id Int64 NOT NULL,
			title Utf8 NOT NULL,
			created_at Timestamp NOT NULL,
			author Utf8,
			PRIMARY KEY (id)
		);
		`,
	)

	if err != nil {
		log.Fatal(err)
	}

	// По сути, таблица links реализует отношение many-to-many,
	// и колонки таблицы являются внешними ключами.
	//
	// Однако YDB не поддерживает конструкцию внешних ключей,
	// поэтому обеспечение согласованности данных - это обязанность разработчика
	err = repo.driver.Query().Exec(
		ctx,
		`
		ALTER TABLE issues ADD COLUMN links_count Uint64;

		CREATE TABLE IF NOT EXISTS links (
			source Int64 NOT NULL,
			destination Int64 NOT NULL,
			PRIMARY KEY (source, destination)
		);
		`,
	)

	if err != nil {
		log.Fatal(err)
	}
}

// Удаляет таблицы issues и links из базы данных
// Используется для очистки схемы перед созданием новой
func (repo *SchemaRepository) DropSchema(ctx context.Context) {
	err := repo.driver.Query().Exec(
		ctx,
		`
			DROP TABLE IF EXISTS issues;
			DROP TABLE IF EXISTS links;
		`,
	)
	if err != nil {
		log.Fatal(err)
	}
}
