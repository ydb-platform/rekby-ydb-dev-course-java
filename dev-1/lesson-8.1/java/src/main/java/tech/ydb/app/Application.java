package tech.ydb.app;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * @author Kirill Kurdyukov
 */
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
            var retryCtx = SessionRetryContext.create(queryClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();
            issueYdbRepository.saveAll(List.of(
                    "Ticket 1",
                    "Ticket 2",
                    "Ticket 3"
                )
            );

            var allTickets = issueYdbRepository.findAll();

            for (var issue : allTickets) {
                issueYdbRepository.updateStatus(issue.id(), "future");
            }

            LOGGER.info("Find by ids [{}, {}]", allTickets.get(0).id(), allTickets.get(1).id());
            for (var issue : issueYdbRepository.findByIds(List.of(allTickets.get(0).id(), allTickets.get(1).id()))) {
                printIssue(issue);
            }

            LOGGER.info("Future issues: ");
            for (var issue : issueYdbRepository.findFutures()) {
                LOGGER.info("Id: {}, Title: {}", issue.id(), issue.title());
            }

            LOGGER.info("Deleted by ids [{}, {}]", allTickets.get(0).id(), allTickets.get(1).id());
            issueYdbRepository.deleteTasks(List.of(allTickets.get(0).id(), allTickets.get(1).id()));

            LOGGER.info("Print all issues: ");
            for (var ticket : issueYdbRepository.findAll()) {
                printIssue(ticket);
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }
}
