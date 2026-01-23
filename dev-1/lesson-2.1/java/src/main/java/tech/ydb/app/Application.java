package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;

import java.time.Duration;

/**
 * @author Kirill Kurdyukov
 */
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    /**
     * Строка подключения к локальной базе данных YDB
     * Формат: grpc://<хост>:<порт>/<путь к базе данных>
     */
    private static final String CONNECTION_STRING = "grpc://127.0.0.1:2136/local";

    public static void main(String[] args) {
        // Создаем драйвер для подключения к YDB через gRPC
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build()
        ) {
            // Создаем клиент для выполнения SQL-запросов
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                // Создаем контекст для автоматических повторных попыток выполнения запросов
                var retryCtx = SessionRetryContext.create(queryClient).build();

                LOGGER.info("Database is available! Result `SELECT 1;` command: {}", new YdbRepository(retryCtx).SelectOne());
            }
        }
    }

    /**
     * Класс для работы с YDB, инкапсулирующий логику выполнения запросов
     */
    public static class YdbRepository {
        private final SessionRetryContext retryCtx;

        public YdbRepository(SessionRetryContext retryCtx) {
            this.retryCtx = retryCtx;
        }

        /**
         * Выполняет простой SQL-запрос SELECT 1 для проверки работоспособности базы данных
         *
         * @return результат запроса (всегда 1)
         */
        public int SelectOne() {
            // Выполняем запрос с автоматическими повторными попытками при ошибках
            QueryReader resultSet = retryCtx.supplyResult(session ->
                    QueryReader.readFrom(session.createQuery("SELECT 1;", TxMode.NONE))
            ).join().getValue();

            // Получаем первый набор результатов
            ResultSetReader resultSetReader = resultSet.getResultSet(0);

            // Переходим к первой строке результата
            resultSetReader.next();

            // Возвращаем значение из первой строки и первой колонки (нумерация с 0)
            return resultSetReader.getColumn(0).getInt32();
        }
    }
}
