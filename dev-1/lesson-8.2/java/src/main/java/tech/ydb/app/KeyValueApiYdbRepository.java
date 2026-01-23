package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.core.Result;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;

/**
 * @author Kirill Kurdyukov
 */
public class KeyValueApiYdbRepository {

    private final SessionRetryContext retryTableCtx;

    public KeyValueApiYdbRepository(SessionRetryContext retryTableCtx) {
        this.retryTableCtx = retryTableCtx;
    }

    /**
     * Массовое добавление или обновление тикетов в таблице.
     */
    public void bulkUpsert(String tableName, List<TitleAuthor> titleAuthorList) {

        // Описывает структуру с полями, которые будут добавляться в таблицу.
        // Смысл операции тот же что для запроса UPSERT. Поля первичного ключа - обязательные, 
        // остальные - опциональные. Если запись с таким первичным ключём уже существует, то
        // переданные поля обновятся, а остальные - сохранят прежние значения.
        var structType = StructType.of(
                "id", PrimitiveType.Int64,
                "title", PrimitiveType.Text,
                "author", PrimitiveType.Text,
                "created_at", OptionalType.of(PrimitiveType.Timestamp)
        );

        var listIssues = ListType.of(structType).newValue(
                titleAuthorList.stream().map(issue -> structType.newValue(
                        "id", PrimitiveValue.newInt64(ThreadLocalRandom.current().nextLong()),
                        "title", PrimitiveValue.newText(issue.title()),
                        "author", PrimitiveValue.newText(issue.author()),
                        "created_at", OptionalType.of(PrimitiveType.Timestamp)
                                .newValue(PrimitiveValue.newTimestamp(Instant.now()))
                )).toList()
        );

        retryTableCtx.supplyStatus(session -> session.executeBulkUpsert(tableName, listIssues))
                .join().expectSuccess();
    }

    /**
     * Чтение всех данных из таблицы.
     * Использует executeReadTable для получения всех записей.
     */
    public List<Issue> readTable(String tableName) {
        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    session.executeReadTable(tableName, ReadTableSettings.newBuilder().build())
                            .start(
                                    readTablePart -> {
                                        var resultSetReader = readTablePart.getResultSetReader();

                                        fetchIssues(listResult, resultSetReader);
                                    }
                            ).join().expectSuccess();


                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    /**
     * Чтение данных из таблицы по ключу.
     * Использует readRows для получения записей по конкретному id.
     */
    public List<Issue> readRows(String tableName, long id) {
        var keyStruct = StructType.of("id", PrimitiveType.Int64);

        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    var resultSetReader = session.readRows(tableName,
                            ReadRowsSettings.newBuilder()
                                    .addKey(keyStruct.newValue("id", PrimitiveValue.newInt64(id)))
                                    .addColumns("id", "title", "created_at", "author")
                                    .build()
                    ).join().getValue().getResultSetReader();

                    fetchIssues(listResult, resultSetReader);

                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    /**
     * Вспомогательный метод для преобразования результатов запроса в объекты Issue.
     * Обрабатывает различные варианты структуры данных (с link_count и status или без них).
     */
    private void fetchIssues(ArrayList<Issue> listResult, ResultSetReader resultSetReader) {
        while (resultSetReader.next()) {
            var id = resultSetReader.getColumn(0).getInt64();

            long linksCount;
            if (resultSetReader.getColumnCount() > 4) {
                linksCount = resultSetReader.getColumn(4).getInt64();
            } else {
                linksCount = 0;
            }

            String status;
            if (resultSetReader.getColumnCount() > 5) {
                status = resultSetReader.getColumn(5).getText();
            } else {
                status = "";
            }

            listResult.add(
                    new Issue(
                            id,
                            resultSetReader.getColumn(1).getText(),
                            resultSetReader.getColumn(2).getTimestamp(),
                            resultSetReader.getColumn(3).getText(),
                            linksCount,
                            status
                    )
            );
        }
    }
}
