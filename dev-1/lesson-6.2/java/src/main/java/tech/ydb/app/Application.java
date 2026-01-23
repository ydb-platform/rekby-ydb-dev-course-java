package tech.ydb.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;

/**
 * Пример обработки файла с использованием топиков YDB
 *
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String PATH = "dev-1/lesson-6.2/java/file.txt";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()
        ) {

            var retryCtx = SessionRetryContext.create(queryClient).build();
            var queryServiceHelper = new QueryServiceHelper(retryCtx);

            dropSchema(queryServiceHelper);
            createSchema(queryServiceHelper);

            // Создаем writer для отправки строк файла в топик
            var writer = topicClient.createSyncWriter(
                    WriterSettings.newBuilder()
                            .setProducerId("producer-file")
                            .setTopicPath("file_topic")
                            .build()
            );

            // Создаем reader для чтения строк из топика
            var reader = topicClient.createSyncReader(
                    ReaderSettings.newBuilder()
                            .setConsumerName("file_consumer")
                            .setTopics(List.of(TopicReadSettings.newBuilder().setPath("file_topic").build()))
                            .build()
            );

            writer.init();
            reader.init();

            String currentDirectory = System.getProperty("user.dir");
            var pathFile = Path.of(currentDirectory, PATH);

            var lines = Files.readAllLines(pathFile);

            // Запускаем фоновое чтение строк из топика
            var readerJob = CompletableFuture.runAsync(() -> runReadJob(reader, retryCtx));

            // Получаем номер последней обработанной строки из таблицы прогресса
            var queryReader = queryServiceHelper.executeQuery("""
                            DECLARE $name AS Text;
                            SELECT line_num FROM write_file_progress WHERE name = $name;
                            """,
                    TxMode.SERIALIZABLE_RW,
                    Params.of("$name", PrimitiveValue.newText(pathFile.toString()))
            );

            var resultSet = queryReader.getResultSet(0);

            long lineNumberLong = 1;
            if (resultSet.next()) {
                lineNumberLong = resultSet.getColumn(0).getInt64();
            }

            var lineNumber = new AtomicLong(lineNumberLong);
            var origLineNumber = new AtomicLong(1);

            // Читаем файл построчно и отправляем в топик длину каждой строки
            lines.forEach(line ->
                    {
                        if (origLineNumber.getAndIncrement() < lineNumber.get()) {
                            return;
                        }

                        // Порядковый номер строки файла используется в качестве
                        // порядкового номера сообщения внутри Producer'а
                        // таким образом при перезапуске процесса сообщения будут повторяться
                        // так, что сервер сможет правильно пропустить дубли.
                        writer.send(Message.newBuilder()
                                .setSeqNo(lineNumber.getAndIncrement())
                                .setData((PATH + ":" + line).getBytes(StandardCharsets.UTF_8))
                                .build()
                        );
                        writer.flush();

                        // Сохраняем прогресс обработки файла в таблицу, чтобы
                        // не повторять работу при перезапусках
                        queryServiceHelper.executeQuery("""
                                        DECLARE $name AS Text;
                                        DECLARE $line_num AS Int64;
                                        UPSERT INTO write_file_progress(name, line_num) VALUES ($name, $line_num);
                                        """,
                                TxMode.SERIALIZABLE_RW,
                                Params.of("$name", PrimitiveValue.newText(pathFile.toString()),
                                        "$line_num", PrimitiveValue.newInt64(lineNumber.get()))
                        );
                    }
            );

            writer.send(Message.newBuilder()
                    .setSeqNo(lineNumber.getAndIncrement())
                    .setData("STOP".getBytes(StandardCharsets.UTF_8))
                    .build()
            );
            writer.flush();

            readerJob.join();
            printTableFile(queryServiceHelper);

            writer.shutdown(10, TimeUnit.SECONDS);
            reader.shutdown();
        }
    }

    private static void runReadJob(SyncReader reader, SessionRetryContext retryCtx) {
        LOGGER.info("Started read worker!");

        var next = true;
        for (int i = 0; next; i++) {
            try {
                var message = reader.receive(10, TimeUnit.SECONDS);

                // Далее идёт пример обработки сообщения из топика ровно 1 раз без использования
                // транзакций для объединения операции с таблицами и топиками - т.е. то, как это
                // делается в других системах, когда нет готовых интеграций.

                // В транзакции будет проверяться было ли уже обработано сообщение и если нет, то
                // обрабатывать и сохранять прогресс. Эти операции должны быть атомарными, чтобы
                // избежать ситуации, когда одно и тоже сообщение будет обработано несколько раз.
                final int finalI = i;
                next = retryCtx.supplyResult(session -> {
                    var curTx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
                    var tx = new TransactionHelper(curTx);
                    assert message != null;
                    var partitionId = message.getPartitionSession().getPartitionId();

                    // Проверяем, не обрабатывали ли мы уже это сообщение
                    var queryReader = tx.executeQuery("""
                            DECLARE $partition_id AS Int64;
                            SELECT last_offset FROM file_progress
                            WHERE partition_id = $partition_id;
                            """, Params.of("$partition_id", PrimitiveValue.newInt64(partitionId))
                    );

                    var resultSet = queryReader.getResultSet(0);
                    var lastOffset = resultSet.next() ? resultSet.getColumn(0).getInt64() : -1;

                    // Если сообщение уже было обработано, пропускаем его
                    if (lastOffset >= message.getOffset()) {
                        message.commit().join();
                        curTx.rollback().join();

                        return CompletableFuture.completedFuture(Result.success(true));
                    }

                    var messageStr = new String(message.getData(), StandardCharsets.UTF_8);
                    if (messageStr.equals("STOP")) {
                        LOGGER.info("Stopped read worker!");

                        return CompletableFuture.completedFuture(Result.success(false));
                    }

                    var messageData = messageStr.split(":");
                    var name = messageData[0];
                    var length = messageData[1].length();

                    if (finalI == 0) {
                        // Сохраняем информацию о строке в таблицу
                        tx.executeQuery("""
                                            DECLARE $name AS Text;
                                            DECLARE $length AS Int64;
                                            INSERT INTO file(name, length) VALUES ($name, $length);
                                        """,
                                Params.of("$name", PrimitiveValue.newText(name), "$length", PrimitiveValue.newInt64(length))
                        );
                    } else {
                        // Обновляем информацию
                        tx.executeQuery("""
                                            DECLARE $name AS Text;
                                            DECLARE $length AS Int64;
                                            UPDATE file SET length = length + $length WHERE name = $name;
                                        """,
                                Params.of("$name", PrimitiveValue.newText(name), "$length", PrimitiveValue.newInt64(length))
                        );
                    }

                    // Обновляем прогресс обработки партиции. Если произойдёт сбой после коммита транзакции,
                    // но до коммита сообщения в топик, то на основе этого прогресса повторная обработка
                    // сообщения будет пропущена.
                    tx.executeQueryWithCommit("""
                                        DECLARE $partition_id AS Int64;
                                        DECLARE $last_offset AS Int64;
                                        UPSERT INTO file_progress(partition_id, last_offset) VALUES ($partition_id, $last_offset);
                                    """,
                            Params.of(
                                    "$partition_id", PrimitiveValue.newInt64(partitionId),
                                    "$last_offset", PrimitiveValue.newInt64(message.getOffset())
                            )
                    );

                    message.commit().join();

                    return CompletableFuture.completedFuture(Result.success(true));
                }).join().getValue();

            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    private static void printTableFile(QueryServiceHelper queryServiceHelper) {
        // Выводим информацию об обработанных строках
        var queryReader = queryServiceHelper.executeQuery(
                "SELECT name, length FROM file;", TxMode.SERIALIZABLE_RW, Params.empty());

        for (ResultSetReader resultSet : queryReader) {
            while (resultSet.next()) {
                LOGGER.info("name: {}, length: {}", resultSet.getColumn(0).getText(), resultSet.getColumn(1).getInt64());
            }
        }
    }

    private static void createSchema(QueryServiceHelper queryServiceHelper) {
        // Создаем таблицы для хранения информации о файле и прогрессе обработки
        queryServiceHelper.executeQuery("""
                CREATE TABLE IF NOT EXISTS file (
                    name Text NOT NULL,
                    length Int64 NOT NULL,
                    PRIMARY KEY (name)
                );
                
                -- Таблица для хранения прогресса обработки партиции
                -- используется для того, чтобы не повторять обработку сообщений
                -- при перезапуске процесса.
                CREATE TABLE IF NOT EXISTS file_progress (
                    partition_id Int64 NOT NULL,
                    last_offset Int64 NOT NULL,
                    PRIMARY KEY (partition_id)
                );
                
                CREATE TABLE IF NOT EXISTS write_file_progress (
                    name Text NOT NULL,
                    line_num Int64 NOT NULL,
                    PRIMARY KEY (name)
                );
                
                CREATE TOPIC IF NOT EXISTS file_topic (
                    CONSUMER file_consumer
                ) WITH(
                    auto_partitioning_strategy='scale_up',
                    min_active_partitions=2,
                    max_active_partitions=5,
                    partition_write_speed_bytes_per_second=5000000
                );
                """);
    }

    private static void dropSchema(QueryServiceHelper queryServiceHelper) {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS file;
                DROP TABLE IF EXISTS file_progress;
                DROP TABLE IF EXISTS write_file_progress;
                DROP TOPIC IF EXISTS file_topic;
                """
        );
    }
}
