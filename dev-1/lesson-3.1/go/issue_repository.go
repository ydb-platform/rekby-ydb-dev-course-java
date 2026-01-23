package main

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var (
	random = rand.New(
		rand.NewSource(time.Now().UnixNano()),
	)
)

// Репозиторий для работы с тикетами в базе данных YDB
// Реализует операции добавления и чтения тикетов
type IssueRepository struct {
	driver *ydb.Driver
}

func NewIssueRepository(driver *ydb.Driver) *IssueRepository {
	return &IssueRepository{
		driver: driver,
	}
}

// Добавление нового тикета в БД
// ctx   [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// title [string] - название тикета
// Возвращает созданный тикет или ошибку
func (repo *IssueRepository) AddIssue(ctx context.Context, title string) (*Issue, error) {
	// Генерируем случайный id для тикета
	id := random.Int63() // do not repeat in production
	timestamp := time.Now()

	// Выполняем UPSERT запрос для добавления тикета
	err := repo.driver.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			err := s.Exec(
				ctx,
				`
				DECLARE $id AS Int64;
				DECLARE $title AS Text;
				DECLARE $created_at AS Timestamp;
				
				UPSERT INTO issues (id, title, created_at)
				VALUES ($id, $title, $created_at);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Int64(id).
						Param("$title").Text(title).
						Param("$created_at").Timestamp(timestamp).
						Build(),
				),
			)
			return err
		},
	)
	if err != nil {
		return nil, err
	}

	return &Issue{
		Id:        id,
		Title:     title,
		Timestamp: timestamp,
	}, nil
}

// Возвращает тикет по заданному id
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// id  [int64] - id тикета
// Возвращает найденный тикет или ошибку
func (repo *IssueRepository) FindById(ctx context.Context, id int64) (*Issue, error) {
	resultIssues := make([]Issue, 0)

	// Выполняем SELECT запрос в режиме [Snapshot Read-Only] для чтения данных
	// Этот режим сообщает серверу, что эта транзакция только для чтения.
	// Это позволяет снизить накладные расходы на подготовку к изменениям
	// и просто читать данные из одного "слепка" базы данных.
	err := repo.driver.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			// Если на предыдущих итерациях функции-ретраера
			// возникла ошибка во время чтения результата,
			// то необходимо очистить уже прочитанные результаты,
			// чтобы избежать дублирования при следующем выполнении функции-ретраера
			resultIssues = make([]Issue, 0)

			queryResult, err := s.Query(
				ctx,
				`
                DECLARE $id AS Int64;
                 
				SELECT
					id,
					title,
					created_at
				FROM issues
				WHERE id=$id;
				`,
				query.WithTxControl(query.SnapshotReadOnlyTxControl()),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Int64(id).
						Build(),
				),
			)

			if err != nil {
				return err
			}

			defer func() { _ = queryResult.Close(ctx) }()

			for {
				resultSet, err := queryResult.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
					if err != nil {
						return err
					}

					resultIssues = append(resultIssues, row)
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if len(resultIssues) > 1 {
		return nil, errors.New("Multiple rows with the same id (lol)")
	}
	if len(resultIssues) == 0 {
		return nil, errors.New("Did not find any issues")
	}
	return &resultIssues[0], nil
}

// Получает все тикеты из базы данных
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
func (repo *IssueRepository) FindAll(ctx context.Context) ([]Issue, error) {
	resultIssues := make([]Issue, 0)

	// Выполняем SELECT запрос в режиме [Snapshot Read-Only] для чтения данных
	// Этот режим сообщает серверу, что эта транзакция только для чтения.
	// Это позволяет снизить накладные расходы на подготовку к изменениям
	// и просто читать данные из одного "слепка" базы данных.
	err := repo.driver.Query().Do(
		ctx,
		func(ctx context.Context, s query.Session) error {
			// Если на предыдущих итерациях функции-ретраера
			// возникла ошибка во время чтения результата,
			// то необходимо очистить уже прочитанные результаты,
			// чтобы избежать смешивания результатов текущей и предыдущей попыток выполнения запросов при ретраях
			resultIssues = make([]Issue, 0)

			queryResult, err := s.Query(
				ctx,
				`
				SELECT
					id,
					title,
					created_at
				FROM issues;
				`,
				query.WithTxControl(query.SnapshotReadOnlyTxControl()),
				query.WithParameters(
					ydb.ParamsBuilder().Build(),
				),
			)

			if err != nil {
				return err
			}

			defer func() { _ = queryResult.Close(ctx) }()

			for {
				resultSet, err := queryResult.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				for row, err := range sugar.UnmarshalRows[Issue](resultSet.Rows(ctx)) {
					if err != nil {
						return err
					}

					resultIssues = append(resultIssues, row)
				}
			}

			return nil
		},
	)
	if err != nil {
		return resultIssues, err
	}

	return resultIssues, nil
}
