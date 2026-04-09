package berlin.yuna.streamline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Execution(ExecutionMode.SAME_THREAD)
class StreamLineBenchmarkTest {

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final int CHEAP_SIZE = 30_000;
    private static final int BLOCKING_SIZE = 4_000;
    private static final int CONCURRENT_PIPELINE_MULTIPLIER = 4;
    private static final int MIN_CONCURRENT_PIPELINES = 4;

    @Test
    void printBenchmarkReportWhenEnabled() throws Exception {
        assumeTrue(Boolean.getBoolean("streamline.benchmark"), "Enable with -Dstreamline.benchmark=true");
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        final int commonPoolParallelism = commonPoolParallelism();
        final int concurrentPipelines = concurrentPipelineCount();

        final List<BenchmarkRow> rows = List.of(
            new BenchmarkRow(
                "Cheap single stream",
                medianNanos(StreamLineBenchmarkTest::runCheapSequentialWorkload),
                medianNanos(StreamLineBenchmarkTest::runCheapParallelWorkload),
                medianNanos(StreamLineBenchmarkTest::runCheapStreamLineWorkload),
                "Cheap CPU-only mapping. Plain Java usually wins."
            ),
            new BenchmarkRow(
                "Cheap scalar count",
                medianNanos(StreamLineBenchmarkTest::runCountSequentialWorkload),
                medianNanos(StreamLineBenchmarkTest::runCountParallelWorkload),
                medianNanos(StreamLineBenchmarkTest::runCountStreamLineWorkload),
                "Fused scalar terminal without materializing terminal results."
            ),
            new BenchmarkRow(
                "Blocking single stream",
                medianNanos(StreamLineBenchmarkTest::runBlockingSequentialWorkload),
                medianNanos(StreamLineBenchmarkTest::runBlockingParallelWorkload),
                medianNanos(StreamLineBenchmarkTest::runBlockingStreamLineWorkload),
                "Blocking work per element. StreamLine should shine."
            ),
            new BenchmarkRow(
                "Concurrent pipelines (common pool x4)",
                medianNanos(StreamLineBenchmarkTest::runConcurrentSequentialWorkload),
                medianNanos(StreamLineBenchmarkTest::runConcurrentParallelCommonPoolWorkload),
                medianNanos(StreamLineBenchmarkTest::runConcurrentStreamLineWorkload),
                "%d blocking pipelines against common-pool parallelism %d.".formatted(concurrentPipelines, commonPoolParallelism)
            )
        );

        System.out.println();
        System.out.println("StreamLine benchmark report");
        System.out.println("Medians are workload-specific and machine-specific. Use them as guidance, not doctrine.");
        System.out.printf("Detected processors: %d%n", availableProcessors);
        System.out.printf("Common pool parallelism: %d%n", commonPoolParallelism);
        System.out.printf("Concurrent pipeline load: %d pipelines (%dx common-pool)%n", concurrentPipelines, CONCURRENT_PIPELINE_MULTIPLIER);
        System.out.println();
        System.out.printf("%-34s %14s %14s %14s  %s%n", "Scenario", "Java Seq", "Java Parallel", "StreamLine", "Note");
        for (final BenchmarkRow row : rows) {
            System.out.printf(
                "%-34s %14s %14s %14s  %s%n",
                row.scenario(),
                formatMillis(row.javaSequentialNanos()),
                formatMillis(row.javaParallelNanos()),
                formatMillis(row.streamLineNanos()),
                row.note()
            );
        }
        System.out.println();
    }

    private static void runCheapSequentialWorkload() {
        assertThat(IntStream.range(0, CHEAP_SIZE).map(value -> value + 1).boxed().toList()).hasSize(CHEAP_SIZE);
    }

    private static void runCheapParallelWorkload() {
        assertThat(IntStream.range(0, CHEAP_SIZE).parallel().map(value -> value + 1).boxed().toList()).hasSize(CHEAP_SIZE);
    }

    private static void runCheapStreamLineWorkload() {
        assertThat(StreamLine.range(0, CHEAP_SIZE).threads(-1).chunks(-1).map(value -> value + 1).toList()).hasSize(CHEAP_SIZE);
    }

    private static void runCountSequentialWorkload() {
        assertThat(IntStream.range(0, CHEAP_SIZE).map(value -> value + 1).filter(value -> value % 2 == 0).count()).isEqualTo(CHEAP_SIZE / 2L);
    }

    private static void runCountParallelWorkload() {
        assertThat(IntStream.range(0, CHEAP_SIZE).parallel().map(value -> value + 1).filter(value -> value % 2 == 0).count()).isEqualTo(CHEAP_SIZE / 2L);
    }

    private static void runCountStreamLineWorkload() {
        assertThat(StreamLine.range(0, CHEAP_SIZE)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .count()).isEqualTo(CHEAP_SIZE / 2L);
    }

    private static void runBlockingSequentialWorkload() {
        assertThat(IntStream.range(0, BLOCKING_SIZE).map(StreamLineBenchmarkTest::blockingOperation).boxed().toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runBlockingParallelWorkload() {
        assertThat(IntStream.range(0, BLOCKING_SIZE).parallel().map(StreamLineBenchmarkTest::blockingOperation).boxed().toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runBlockingStreamLineWorkload() {
        assertThat(StreamLine.range(0, BLOCKING_SIZE).threads(-1).chunks(-1).map(StreamLineBenchmarkTest::blockingOperation).toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runConcurrentSequentialWorkload() {
        runConcurrentWorkload(
            () -> IntStream.range(0, concurrentPipelineSize()).map(StreamLineBenchmarkTest::blockingOperation).boxed().toList(),
            "Concurrent sequential benchmark failed"
        );
    }

    private static void runConcurrentParallelCommonPoolWorkload() {
        runConcurrentWorkload(
            () -> IntStream.range(0, concurrentPipelineSize()).parallel().map(StreamLineBenchmarkTest::blockingOperation).boxed().toList(),
            "Concurrent common-pool benchmark failed"
        );
    }

    private static void runConcurrentStreamLineWorkload() {
        runConcurrentWorkload(
            () -> StreamLine.range(0, concurrentPipelineSize()).threads(-1).chunks(-1).map(StreamLineBenchmarkTest::blockingOperation).toList(),
            "Concurrent StreamLine benchmark failed"
        );
    }

    private static void runConcurrentWorkload(final Callable<List<Integer>> task, final String errorMessage) {
        final ExecutorService wrapperExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("benchmark-wrapper-", 0).factory());
        try {
            final List<Callable<List<Integer>>> tasks = new ArrayList<>();
            for (int pipeline = 0; pipeline < concurrentPipelineCount(); pipeline++) {
                tasks.add(task);
            }
            final List<Future<List<Integer>>> futures = wrapperExecutor.invokeAll(tasks);
            for (final Future<List<Integer>> future : futures) {
                assertThat(future.get(20, TimeUnit.SECONDS)).hasSize(concurrentPipelineSize());
            }
        } catch (final Exception exception) {
            throw new IllegalStateException(errorMessage, exception);
        } finally {
            wrapperExecutor.shutdown();
            awaitTermination(wrapperExecutor);
        }
    }

    private static int blockingOperation(final int value) {
        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
        return value;
    }

    private static long medianNanos(final Runnable workload) {
        for (int run = 0; run < WARMUP_RUNS; run++) {
            workload.run();
        }

        final long[] durations = new long[MEASURED_RUNS];
        for (int run = 0; run < MEASURED_RUNS; run++) {
            final long start = System.nanoTime();
            workload.run();
            durations[run] = System.nanoTime() - start;
        }
        Arrays.sort(durations);
        return durations[MEASURED_RUNS / 2];
    }

    private static String formatMillis(final Long nanos) {
        return nanos == null ? "-" : String.format("%.2f ms", nanos / 1_000_000.0);
    }

    private static int commonPoolParallelism() {
        return Math.max(1, ForkJoinPool.getCommonPoolParallelism());
    }

    private static int concurrentPipelineCount() {
        return Math.max(MIN_CONCURRENT_PIPELINES, commonPoolParallelism() * CONCURRENT_PIPELINE_MULTIPLIER);
    }

    private static int concurrentPipelineSize() {
        return BLOCKING_SIZE / 4;
    }

    private static void awaitTermination(final ExecutorService executor) {
        try {
            assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Benchmark executor termination interrupted", exception);
        }
    }

    private record BenchmarkRow(
        String scenario,
        Long javaSequentialNanos,
        Long javaParallelNanos,
        Long streamLineNanos,
        String note
    ) {
    }
}
