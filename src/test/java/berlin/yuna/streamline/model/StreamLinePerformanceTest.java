package berlin.yuna.streamline.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Execution(ExecutionMode.SAME_THREAD)
class StreamLinePerformanceTest {

    private static ExecutorService executorService;
    static final int STREAM_SIZE = 10;
    final static int TASK_SIZE = 10;

    @BeforeEach
    void setUp() {
        executorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("virtual-thread-", 0).factory());
    }

    @AfterEach
    void tearDown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (final InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testArguments")
    void performanceTest(final String testName, final ExRunnable task) throws Exception {
        task.run();
    }

    static Stream<Arguments> testArguments() {
        return Stream.of(
            Arguments.of("Loop [for]", (ExRunnable) StreamLinePerformanceTest::testNoStream),
            Arguments.of(Stream.class.getSimpleName() + " [Sequential]", (ExRunnable) StreamLinePerformanceTest::testStream),
            Arguments.of(Stream.class.getSimpleName() + "Stream [Parallel]", (ExRunnable) StreamLinePerformanceTest::testParallelStream),
            Arguments.of(StreamLine.class.getSimpleName() + " [Ordered]", (ExRunnable) StreamLinePerformanceTest::testStreamLine_with_order),
            Arguments.of(StreamLine.class.getSimpleName() + " [Unordered]", (ExRunnable) StreamLinePerformanceTest::testStreamLine_without_order),
            Arguments.of(StreamLine.class.getSimpleName() + " [2 Threads]", (ExRunnable) StreamLinePerformanceTest::testStreamLine_with_twoThreads)
        );
    }

    static void testStreamLine_without_order() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                StreamLine.range(executorService, 0, STREAM_SIZE).map(StreamLinePerformanceTest::slowProcessing).toArray();
            }));
        }

        final List<Future<Object>> futures = executorService.invokeAll(tasks);
        assertTrue(futures.stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    static void testStreamLine_with_order() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                StreamLine.range(executorService, 0, STREAM_SIZE).ordered(true).map(StreamLinePerformanceTest::slowProcessing).toArray();
            }));
        }

        assertTrue(executorService.invokeAll(tasks).stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    static void testStreamLine_with_twoThreads() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                StreamLine.range(executorService, 0, STREAM_SIZE).threads(2).map(StreamLinePerformanceTest::slowProcessing).toArray();
            }));
        }

        assertTrue(executorService.invokeAll(tasks).stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    static void testStream() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                IntStream.range(0, STREAM_SIZE).boxed().map(StreamLinePerformanceTest::slowProcessing).toArray();
            }));
        }

        assertTrue(executorService.invokeAll(tasks).stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    static void testParallelStream() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                IntStream.range(0, STREAM_SIZE).parallel().boxed().map(StreamLinePerformanceTest::slowProcessing).toArray();
            }));
        }

        assertTrue(executorService.invokeAll(tasks).stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    static void testNoStream() throws InterruptedException {
        final List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < TASK_SIZE; i++) {
            tasks.add(Executors.callable(() -> {
                for (int j = 0; j < STREAM_SIZE; j++) {
                    slowProcessing(j);
                }
            }));
        }

        assertTrue(executorService.invokeAll(tasks).stream().allMatch(Future::isDone), "All tasks should complete successfully");
    }

    private static long slowProcessing(final int n) {
        try {
            Thread.sleep(100); // Simulate a delay in processing
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return System.currentTimeMillis();
    }
}
