@_spi(ConnectionPool) import PostgresNIO
import InMemoryTracing
import Logging
import NIOPosix
import Tracing
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PostgresClientTracingTests: XCTestCase {
    func testQueryTracingIncludesLeaseWait() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger
        let query: PostgresQuery = "SELECT 1;"

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.minimumConnections = 0
            config.options.maximumConnections = 1
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                let (leaseAcquiredStream, leaseAcquiredContinuation) = AsyncStream.makeStream(of: Void.self)
                let (releaseLeaseStream, releaseLeaseContinuation) = AsyncStream.makeStream(of: Void.self)
                let leaseTask = Task {
                    try await client.withConnection { _ in
                        leaseAcquiredContinuation.yield()
                        leaseAcquiredContinuation.finish()

                        var releaseLeaseIterator = releaseLeaseStream.makeAsyncIterator()
                        await releaseLeaseIterator.next()
                    }
                }

                var leaseAcquiredIterator = leaseAcquiredStream.makeAsyncIterator()
                await leaseAcquiredIterator.next()

                let queryTask = Task {
                    let rows = try await client.query(query, logger: logger)
                    for try await _ in rows {}
                }

                let didStartQuerySpan = try await self.waitForActiveSpan(withQueryText: query.sql, in: tracer)
                if didStartQuerySpan {
                    try await Task.sleep(for: .milliseconds(100))
                }

                releaseLeaseContinuation.yield()
                releaseLeaseContinuation.finish()

                try await leaseTask.value
                try await queryTask.value

                group.cancelAll()

                XCTAssertTrue(didStartQuerySpan)
            }
        }

        let querySpan = try XCTUnwrap(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == query.sql
        }))

        XCTAssertEqual(querySpan.kind, SpanKind.client)

        let duration = querySpan.endInstant.nanosecondsSinceEpoch - querySpan.startInstant.nanosecondsSinceEpoch
        XCTAssertGreaterThanOrEqual(duration, 80_000_000)
    }

    func testTransactionTracingParentsDatabaseSpans() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                try await client.withTransaction(logger: logger) { transaction in
                    let rows = try await transaction.query("SELECT 1;", logger: logger)
                    for try await _ in rows {}
                }

                group.cancelAll()
            }
        }

        let transactionSpan = try XCTUnwrap(
            tracer.finishedSpans.first(where: { $0.operationName == "postgres.transaction" })
        )
        let beginSpan = try XCTUnwrap(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "BEGIN;"
        }))
        let selectSpan = try XCTUnwrap(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "SELECT 1;"
        }))
        let commitSpan = try XCTUnwrap(tracer.finishedSpans.first(where: {
            $0.attributes.stringValue(for: "db.query.text") == "COMMIT;"
        }))

        XCTAssertEqual(transactionSpan.kind, .internal)
        XCTAssertEqual(beginSpan.parentSpanID, transactionSpan.spanID)
        XCTAssertEqual(selectSpan.parentSpanID, transactionSpan.spanID)
        XCTAssertEqual(commitSpan.parentSpanID, transactionSpan.spanID)
    }

    func testTransactionTracingFailsWhenLeaseFails() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            await withTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                group.cancelAll()
                try? await Task.sleep(for: .milliseconds(10))

                do {
                    try await client.withTransaction(logger: logger) { _ in
                        ()
                    }
                    XCTFail("Expected `withTransaction` to throw after client shutdown")
                } catch {
                    // expected
                }
            }
        }

        let transactionSpan = try XCTUnwrap(
            tracer.finishedSpans.first(where: { $0.operationName == "postgres.transaction" })
        )
        XCTAssertEqual(transactionSpan.kind, .internal)
        XCTAssertEqual(transactionSpan.status?.code, .error)
        XCTAssertNotNil(transactionSpan.attributes["error.type"])
    }

    func testKeepAliveQueriesAreNotTraced() async throws {
        let tracer = InMemoryTracer()
        var rawLogger = Logger(label: "PostgresClientTracingTests")
        rawLogger.logLevel = .debug
        let logger = rawLogger
        let keepAliveValue = "postgresnio-keepalive-tracing"
        let keepAliveQuery = PostgresQuery(
            unsafeSQL: "SELECT set_config('application_name', 'postgresnio-keepalive-tracing', false);"
        )
        let resetQuery = PostgresQuery(
            unsafeSQL: "SELECT set_config('application_name', 'postgresnio-user-query', false);"
        )
        let verifyQuery = PostgresQuery(
            unsafeSQL: "SELECT current_setting('application_name');"
        )

        try await self.withEventLoopGroup { eventLoopGroup in
            try await self.verifyDatabaseAccess(on: eventLoopGroup)

            var config = PostgresClient.Configuration.makeTestConfiguration()
            config.options.minimumConnections = 1
            config.options.maximumConnections = 1
            config.options.keepAliveBehavior = .init(
                frequency: .milliseconds(100),
                query: keepAliveQuery
            )
            config.options.tracing = .init(isEnabled: true, queryTextPolicy: .recordAll)
            config.options.tracing.tracer = tracer

            let client = PostgresClient(configuration: config, eventLoopGroup: eventLoopGroup, backgroundLogger: logger)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await client.run()
                }

                let resetRows = try await client.query(resetQuery, logger: logger).decode(String.self)
                for try await _ in resetRows {}

                let didFinishResetSpan = try await self.waitForFinishedSpan(
                    withQueryText: resetQuery.sql,
                    in: tracer
                )
                XCTAssertTrue(didFinishResetSpan)
                tracer.clearFinishedSpans()

                try await Task.sleep(for: .milliseconds(250))

                let rows = try await client.query(verifyQuery, logger: logger)

                var values = [String]()
                for try await value in rows.decode(String.self) {
                    values.append(value)
                }

                let didFinishVerifySpan = try await self.waitForFinishedSpan(
                    withQueryText: verifyQuery.sql,
                    in: tracer
                )
                XCTAssertTrue(didFinishVerifySpan)

                XCTAssertEqual(values, [keepAliveValue])
                XCTAssertEqual(tracer.finishedSpans.count, 1)
                XCTAssertEqual(
                    tracer.finishedSpans.first?.attributes.stringValue(for: "db.query.text"),
                    verifyQuery.sql
                )
                XCTAssertFalse(tracer.finishedSpans.contains(where: {
                    $0.attributes.stringValue(for: "db.query.text") == keepAliveQuery.sql
                }))

                group.cancelAll()
            }
        }
    }

    private func verifyDatabaseAccess(on eventLoopGroup: MultiThreadedEventLoopGroup) async throws {
        let connection = try await PostgresConnection.test(on: eventLoopGroup.next()).get()
        try await connection.close()
    }

    private func withEventLoopGroup<T>(
        _ body: (MultiThreadedEventLoopGroup) async throws -> T
    ) async throws -> T {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        do {
            let result = try await body(eventLoopGroup)
            try await eventLoopGroup.shutdownGracefully()
            return result
        } catch {
            try? await eventLoopGroup.shutdownGracefully()
            throw error
        }
    }

    private func waitForFinishedSpan(
        withQueryText queryText: String,
        in tracer: InMemoryTracer
    ) async throws -> Bool {
        for _ in 0..<50 {
            if tracer.finishedSpans.contains(where: {
                $0.attributes.stringValue(for: "db.query.text") == queryText
            }) {
                return true
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }

    private func waitForActiveSpan(
        withQueryText queryText: String,
        in tracer: InMemoryTracer
    ) async throws -> Bool {
        for _ in 0..<50 {
            if tracer.activeSpans.contains(where: {
                $0.attributes.stringValue(for: "db.query.text") == queryText
            }) {
                return true
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }
}

private extension SpanAttributes {
    func stringValue(for key: String) -> String? {
        switch self[key]?.toSpanAttribute() {
        case .string(let value):
            return value
        case .stringConvertible(let value):
            return String(describing: value)
        default:
            return nil
        }
    }
}
