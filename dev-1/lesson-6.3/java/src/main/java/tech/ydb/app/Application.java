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
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.SendSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;

/**
 * Пример обработки файла с использованием транзакционных операций с топиками YDB
 *
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String PATH = "/lesson-6.3/java/file.txt";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) throws IOException {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()) {

            var retryCtx = SessionRetryContext.create(queryClient).build();
            var queryServiceHelper = new QueryServiceHelper(retryCtx);

            dropSchema(queryServiceHelper);
            createSchema(queryServiceHelper);

            var reader = topicClient.createSyncReader(
                    ReaderSettings.newBuilder()
                            .setConsumerName("file_consumer")
                            .setTopics(List.of(TopicReadSettings.newBuilder().setPath("file_topic").build()))
                            .build()
            );

            reader.init();

            String currentDirectory = System.getProperty("user.dir");
            var pathFile = Path.of(currentDirectory, PATH);
            var lines = Files.readAllLines(pathFile);
            var readerJob = CompletableFuture.runAsync(() -> runTransactionReadJob(reader, retryCtx));
            var queryReader = queryServiceHelper.executeQuery("""
                            DECLARE $name AS Text;
                            SELECT line_num FROM write_file_progress
                            WHERE name = $name;
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

            // Читаем файл построчно и отправляем строки в топик в рамках транзакции
            lines.forEach(line -> {
                        if (origLineNumber.getAndIncrement() < lineNumber.get()) {
                            return;
                        }

                        var lineNumberCur = lineNumber.getAndIncrement();
                        retryCtx.supplyStatus(
                                session -> {
                                    // Начинаем интерактивную транзакцию
                                    var transaction = session.beginTransaction(TxMode.SERIALIZABLE_RW).join().getValue();
                                    var tx = new TransactionHelper(transaction);
                                    // При транзакционной записи нужно создавать писателя для каждой транзакции
                                    // иначе встретиться со сложными для отладки проблемами в виде внезапной
                                    // остановки писателя и необходимости его пересоздания.

                                    // Транзакций обычно много, поэтому producerID нужно указывать явно - чтобы
                                    // не перегружать кластер их большим количеством.
                                    // Важно чтобы producerID был уникальным в каждый момент времени,
                                    // т.к. при параллельном подключении двух писателей с одинаковым ProducerID
                                    // один из них получит ошибку и будет закрыт.
                                    var writer = topicClient.createSyncWriter(
                                            WriterSettings.newBuilder()
                                                    .setProducerId("producer-file")
                                                    .setTopicPath("file_topic")
                                                    .build()
                                    );
                                    writer.initAndWait();

                                    // Отправляем сообщение в топик в рамках транзакции
                                    writer.send(
                                            Message.newBuilder()
                                                    .setSeqNo(lineNumberCur)
                                                    .setData((PATH + ":" + line).getBytes(StandardCharsets.UTF_8))
                                                    .build(),
                                            SendSettings.newBuilder().setTransaction(transaction).build()
                                    );
                                    writer.flush();

                                    tx.executeQuery("""
                                                    DECLARE $name AS Text;
                                                    DECLARE $line_num AS Int64;
                                                    UPSERT INTO write_file_progress(name, line_num) VALUES ($name, $line_num);
                                                    """,
                                            Params.of("$name", PrimitiveValue.newText(pathFile.toString()),
                                                    "$line_num", PrimitiveValue.newInt64(lineNumberCur))
                                    );

                                    // Фиксируем транзакцию.
                                    // В этот момент транзакция будет завершена и гарантируется атомарность операций
                                    // и с топиками и с таблицами, т.е. можно работать естественным для БД образом даже
                                    // если в операциях теперь участвует очередь сообщений (топик).
                                    transaction.commit().join();

                                    try {
                                        writer.shutdown(10, TimeUnit.SECONDS);
                                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return CompletableFuture.completedFuture(Status.SUCCESS);
                                }
                        ).join().expectSuccess();
                    }
            );
            sendStopMessage(topicClient, lineNumber.getAndIncrement());


            readerJob.join();
            reader.shutdown();

            printTableFile(queryServiceHelper);
        }
    }

    private static void sendStopMessage(TopicClient topicClient, long lastSeqNo) {
        var writer = topicClient.createSyncWriter(
                WriterSettings.newBuilder()
                        .setProducerId("producer-file")
                        .setTopicPath("file_topic")
                        .build()
        );
        writer.initAndWait();
        writer.send(
                Message.newBuilder()
                        .setSeqNo(lastSeqNo)
                        .setData(("STOP").getBytes(StandardCharsets.UTF_8))
                        .build()
        );
        writer.flush();
        try {
            writer.shutdown(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runTransactionReadJob(SyncReader reader, SessionRetryContext retryCtx) {
        LOGGER.info("Started read worker!");

        var next = true;
        while (next) {
            try {
                // Обрабатываем сообщения в транзакционном режиме
                next = retryCtx.supplyResult(session -> {
                    // Начинаем интерактивную транзакцию
                    var transaction = session.beginTransaction(TxMode.SERIALIZABLE_RW).join().getValue();
                    var tx = new TransactionHelper(transaction);

                    tech.ydb.topic.read.Message message;
                    try {
                        // Читаем сообщение в рамках транзакции
                        message = reader.receive(ReceiveSettings.newBuilder()
                                .setTransaction(transaction)
                                .setTimeout(1, TimeUnit.SECONDS)
                                .build()
                        );
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    // Если сообщение не найдено, то откатываем транзакцию
                    if (message == null) {
                        transaction.rollback().join();
                        return CompletableFuture.completedFuture(Result.success(true));
                    }

                    var messageStr = new String(message.getData(), StandardCharsets.UTF_8);
                    if (messageStr.equals("STOP")) {
                        LOGGER.info("Stopped read worker!");

                        return CompletableFuture.completedFuture(Result.success(false));
                    }

                    var messageData = new String(message.getData(), StandardCharsets.UTF_8).split(":");
                    var name = messageData[0];
                    var length = messageData[1].length();
                    var lineNumber = message.getSeqNo();

                    tx.executeQuery("""
                                        DECLARE $name AS Text;
                                        DECLARE $line AS Int64;
                                        DECLARE $length AS Int64;
                                        UPSERT INTO file(name, line, length) VALUES ($name, $line, $length);
                                    """,
                            Params.of(
                                    "$name", PrimitiveValue.newText(name),
                                    "$line", PrimitiveValue.newInt64(lineNumber),
                                    "$length", PrimitiveValue.newInt64(length)
                            )
                    );
                    // Фиксируем транзакцию
                    // В этот момент транзакция будет завершена и гарантируется атомарность операций
                    // и с топиками и с таблицами, т.е. можно работать естественным для БД образом даже
                    // если в операциях теперь участвует очередь сообщений (топик).
                    transaction.commit().join();

                    return CompletableFuture.completedFuture(Result.success(true));
                }).join().getValue();
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    private static void printTableFile(QueryServiceHelper queryServiceHelper) {
        var queryReader = queryServiceHelper.executeQuery(
                "SELECT name, line, length FROM file;", TxMode.SERIALIZABLE_RW, Params.empty()
        );

        for (ResultSetReader resultSet : queryReader) {
            while (resultSet.next()) {
                LOGGER.info(
                        "name: " + resultSet.getColumn(0).getText() +
                                ", line: " + resultSet.getColumn(1).getInt64() +
                                ", length: " + resultSet.getColumn(2).getInt64()
                );
            }
        }
    }

    private static void createSchema(QueryServiceHelper queryServiceHelper) {
        queryServiceHelper.executeQuery("""
                CREATE TABLE IF NOT EXISTS file (
                    name Text NOT NULL,
                    line Int64 NOT NULL,
                    length Int64 NOT NULL,
                    PRIMARY KEY (name, line)
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
                """
        );
    }

    private static void dropSchema(QueryServiceHelper queryServiceHelper) {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS file;
                DROP TABLE IF EXISTS write_file_progress;
                DROP TOPIC IF EXISTS file_topic;
                """
        );
    }
}
