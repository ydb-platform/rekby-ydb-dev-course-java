package issue

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	query "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var (
	random = rand.New(
		rand.NewSource(time.Now().UnixNano()),
	)
)

type IssueRepository struct {
	driver *ydb.Driver
}

func NewIssueRepository(driver *ydb.Driver) *IssueRepository {
	return &IssueRepository{
		driver: driver,
	}
}

// Добавление нового тикета в БД
// ctx   	[context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// title	[string] - название тикета
// author	[string] - автор тикета
// Возвращает созданный тикет или ошибку
func (repo *IssueRepository) AddIssue(
	ctx context.Context,
	title string,
	author string,
) (*Issue, error) {
	// Генерируем случайный id для тикета
	id := random.Int63() // do not repeat in production
	timestamp := time.Now()

	// Выполняем UPSERT запрос для добавления тикета
	var err = repo.driver.Query().Do(
		ctx,
		func(ctx context.Context, session query.Session) error {
			err := session.Exec(
				ctx,
				`
				DECLARE $id AS Int64;
				DECLARE $title AS Utf8;
				DECLARE $created_at AS Timestamp;
				DECLARE $author as Utf8;
				
				UPSERT INTO issues (id, title, created_at, author)
				VALUES ($id, $title, $created_at, $author);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Int64(id).
						Param("$title").Text(title).
						Param("$created_at").Timestamp(timestamp).
						Param("$author").Text(author).
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
		Author:    author,
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
		func(ctx context.Context, session query.Session) error {
			// Если на предыдущих итерациях функции-ретраера
			// возникла ошибка во время чтения результата,
			// то необходимо очистить уже прочитанные результаты,
			// чтобы избежать дублирования при следующем выполнении функции-ретраера
			resultIssues = make([]Issue, 0)

			queryResult, err := session.Query(
				ctx,
				`
                DECLARE $id AS Int64;
                 
				SELECT
					id,
					title,
					created_at,
					author,
					COALESCE(links_count, 0) AS links_count
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
		func(ctx context.Context, session query.Session) error {
			// Если на предыдущих итерациях функции-ретраера
			// возникла ошибка во время чтения результата,
			// то необходимо очистить уже прочитанные результаты,
			// чтобы избежать смешивания результатов текущей и предыдущей попыток выполнения запросов при ретраях
			resultIssues = make([]Issue, 0)

			queryResult, err := session.Query(
				ctx,
				`
				SELECT
					id,
					title,
					created_at,
					author,
					COALESCE(links_count, 0) AS links_count
				FROM issues;
				`,
				query.WithTxControl(query.SnapshotReadOnlyTxControl()),
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

// Следующие 2 функции демонстрируют разницу между неинтерактивными и интерактивными транзакциями:
//
// LinkTicketsNoInteractive - показывает использование неинтерактивной транзакции
// LinkTicketsInteractive - показывает использование интерактивной транзакции

// Создает связь между двумя тикетами, используя неинтерактивную транзакцию
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// id1 [int64] - id первого тикета для создания связи
// id2 [int64] - id второго тикета для создания связи
// Возвращает связи между тикетами или ошибку
func (repo *IssueRepository) LinkTicketsNoInteractive(
	ctx context.Context,
	id1 int64,
	id2 int64,
) ([]IssueLinksCount, error) {
	var resultLinks = make([]IssueLinksCount, 0)

	// Здесь показано использование неинтерактивной транзакции:
	// мы выполняем один сетевой запрос к серверу YDB,
	// в рамках которого выполнится несколько YQL-запросов к базе данных
	var err = repo.driver.Query().Do(
		ctx,
		func(ctx context.Context, session query.Session) error {
			// Если на предыдущих итерациях функции-ретраера
			// возникла ошибка во время чтения результата,
			// то необходимо очистить уже прочитанные результаты,
			// чтобы избежать смешивания результатов текущей и предыдущей попыток выполнения запросов при ретраях
			resultLinks = make([]IssueLinksCount, 0)

			queryResult, err := session.Query(
				ctx,
				`
				DECLARE $t1 as Int64;
				DECLARE $t2 as Int64;

				UPDATE issues
				SET links_count = COALESCE(links_count, 0) + 1
				WHERE id IN ($t1, $t2);

				INSERT INTO links (source, destination)
				VALUES ($t1, $t2), ($t2, $t1);

				SELECT id, links_count FROM issues
				WHERE id in ($t1, $t2);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Int64(id1).
						Param("$t2").Int64(id2).
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

				for row, err := range sugar.UnmarshalRows[IssueLinksCount](resultSet.Rows(ctx)) {
					if err != nil {
						return err
					}

					resultLinks = append(resultLinks, row)
				}
			}

			return nil
		},
	)
	if err != nil {
		return resultLinks, err
	}

	return resultLinks, nil
}

// Создает связь между двумя тикетами, используя интерактивную транзакцию
// ctx [context.Context] - контекст для управления исполнением запроса (например, можно задать таймаут)
// id1 [int64] - id первого тикета для создания связи
// id2 [int64] - id второго тикета для создания связи
// Возвращает связи между тикетами или ошибку
func (repo *IssueRepository) LinkTicketsInteractive(
	ctx context.Context,
	id1 int64,
	id2 int64,
) ([]IssueLinksCount, error) {
	var resultLinks = make([]IssueLinksCount, 0)

	// Здесь показано использование интерактивной транзакции.
	// Отличие интерактивной транзакции в том, что мы можем в рамках одной транзакции
	// последовательно выполнять **несколько** сетевых запросов к серверу YDB,
	// в каждом из которых выполнится один или несколько YQL-запросов к базе данных.
	//
	// То есть интерактивные транзакции дают нам возможность выполнить один запрос,
	// прочитать его результат и принять решение, какой запрос выполнить дальше.
	// Транзакция при этом останется открытой до тех пор, пока мы сами её не закроем.
	//
	// Это наиболее гибкий вариант работы с базой данных,
	// однако он приводит к накладным расходам со стороны сервера,
	// потому что необходимо поддерживать распределенную транзакцию открытой неопределенное время.
	//
	// Также не стоит забывать, что использование интерактивных транзакций
	// ухудшает читаемость кода клиентской программы
	var err = repo.driver.Query().DoTx(
		ctx,
		// Если данная лямбда вернет nil вместо ошибки,
		// то транзакция будет закоммичена.
		// В противном случае транзакция будет откачена.
		func(ctx context.Context, tx query.TxActor) error {
			err := tx.Exec(
				ctx,
				`
				DECLARE $t1 AS Int64;
				DECLARE $t2 AS Int64;

				UPDATE issues
				SET links_count = COALESCE(links_count, 0) + 1
				WHERE id in ($t1, $t2);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Int64(id1).
						Param("$t2").Int64(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			err = tx.Exec(
				ctx,
				`
				DECLARE $t1 as Int64;
				DECLARE $t2 as Int64;

				INSERT INTO links (source, destination)
				VALUES ($t1, $t2), ($t2, $t1);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Int64(id1).
						Param("$t2").Int64(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			queryResult, err := tx.QueryResultSet(
				ctx,
				`
				DECLARE $t1 as Int64;
				DECLARE $t2 as Int64;

				SELECT id, links_count FROM issues
				WHERE id IN ($t1, $t2);
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$t1").Int64(id1).
						Param("$t2").Int64(id2).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			defer func() { _ = queryResult.Close(ctx) }()

			for row, err := range sugar.UnmarshalRows[IssueLinksCount](queryResult.Rows(ctx)) {
				if err != nil {
					return err
				}

				resultLinks = append(resultLinks, row)
			}
			return nil
		},
	)

	if err != nil {
		return resultLinks, err
	}

	return resultLinks, nil
}
