package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import javax.annotation.Nullable;

/**
 * Репозиторий для работы с тикетами в базе данных YDB
 * Реализует операции добавления и чтения тикетов
 *
 * @author Kirill Kurdyukov
 */
public class IssueYdbRepository {

    // Контекст для автоматических повторных попыток выполнения запросов
    // Принимается извне через конструктор для:
    // 1. Следования принципу Dependency Injection - зависимости класса передаются ему извне
    // 2. Улучшения тестируемости - можно передать mock-объект для тестов
    // 3. Централизованного управления конфигурацией ретраев
    // 4. Возможности переиспользования одного контекста для разных репозиториев
    private final QueryServiceHelper queryServiceHelper;

    public IssueYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    /**
     * Добавляет новый тикет в базу данных
     *
     * @param title название тикета
     * @return созданный тикет со сгенерированным ID и временем создания
     */
    public Issue addIssue(String title) {
        // Генерируем случайный ID для тикета
        var id = ThreadLocalRandom.current().nextLong();
        var now = Instant.now();

        // Выполняем UPSERT запрос для добавления тикета
        // Изменять данные можно только в режиме транзакции SERIALIZABLE_RW, поэтому используем его
        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DECLARE $title AS Text;
                        DECLARE $created_at AS Timestamp;
                        UPSERT INTO issues (id, title, created_at)
                        VALUES ($id, $title, $created_at);
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of(
                        "$id", PrimitiveValue.newInt64(id),
                        "$title", PrimitiveValue.newText(title),
                        "$created_at", PrimitiveValue.newTimestamp(now)
                )
        );

        return new Issue(id, title, now);
    }

    /**
     * Возвращает тикет по заданному id
     *
     * @return найденный тикет или null, если тикет с указанным id не найден
     */
    @Nullable
    public Issue findById(long id) {
        // Выполняем SELECT запрос в режиме SNAPSHOT_RO для чтения данных
        // Этот режим сообщает серверу, что это транзакция только для чтения.
        // Это позволяет снизить накладные расходы на подготовку к изменениям и просто читать данные из
        // одного снимка базы данных.
        var resultSet = queryServiceHelper.executeQuery("SELECT id, title, created_at FROM issues WHERE id=$id;",
                TxMode.SNAPSHOT_RO, Params.of("$id", PrimitiveValue.newInt64(id)));

        var resultSetReader = resultSet.getResultSet(0);

        return resultSetReader.next() ? new Issue(
                resultSetReader.getColumn(0).getInt64(),
                resultSetReader.getColumn(1).getText(),
                resultSetReader.getColumn(2).getTimestamp()
        ) : null;
    }

    /**
     * Получает все тикеты из базы данных
     *
     * @return список всех тикетов
     */
    public List<Issue> findAll() {
        var titles = new ArrayList<Issue>();
        // Выполняем SELECT запрос в режиме SNAPSHOT_RO для чтения данных
        // Этот режим сообщает серверу, что это транзакция только для чтения.
        // Это позволяет снизить накладные расходы на подготовку к изменениям и просто читать данные из 
        // одного снимка базы данных.
        var resultSet = queryServiceHelper.executeQuery("SELECT id, title, created_at FROM issues;",
                TxMode.SNAPSHOT_RO, Params.empty());

        var resultSetReader = resultSet.getResultSet(0);

        // Читаем все строки результата
        while (resultSetReader.next()) {
            titles.add(new Issue(
                    resultSetReader.getColumn(0).getInt64(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp()
            ));
        }

        return titles;
    }
}