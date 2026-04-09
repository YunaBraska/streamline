package berlin.yuna.streamline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class StreamLinePerformanceTest {

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 5;
    private static final int LIGHTWEIGHT_SIZE = 30_000;
    private static final int BLOCKING_SIZE = 4_000;

    @Test
    void shouldReduceSchedulingOverheadWhenChunkingUnlimitedWork() {
        final long perItemDuration = medianNanos(StreamLinePerformanceTest::runUnlimitedPerItemWorkload);
        final long autoChunkDuration = medianNanos(StreamLinePerformanceTest::runUnlimitedAutoChunkWorkload);
        final long explicitChunkDuration = medianNanos(StreamLinePerformanceTest::runUnlimitedExplicitChunkWorkload);

        assertThat(autoChunkDuration).isLessThan(perItemDuration);
        assertThat(explicitChunkDuration).isLessThan(perItemDuration);
    }

    @Test
    void shouldStayCompetitiveForBlockingWorkloadsWhenChunkingBoundedWorkers() {
        final long perItemDuration = medianNanos(() -> runBlockingWorkload(1));
        final long chunkedDuration = medianNanos(() -> runBlockingWorkload(16));

        assertThat(chunkedDuration).isLessThanOrEqualTo((long) (perItemDuration * 1.25));
    }

    @Test
    void shouldCompareBlockingWorkloadAgainstJavaStreamsAndStreamLine() {
        final long sequentialDuration = medianNanos(StreamLinePerformanceTest::runSequentialBlockingWorkload);
        final long parallelDuration = medianNanos(StreamLinePerformanceTest::runParallelBlockingWorkload);
        final long streamLineDuration = medianNanos(StreamLinePerformanceTest::runStreamLineBlockingWorkload);

        assertThat(parallelDuration)
            .withFailMessage("Expected parallel stream to beat sequential stream for the blocking workload, but sequential=%d ns parallel=%d ns streamLine=%d ns", sequentialDuration, parallelDuration, streamLineDuration)
            .isLessThan(sequentialDuration);
        assertThat(streamLineDuration)
            .withFailMessage("Expected StreamLine to stay at least competitive with java parallel stream for the blocking workload, but sequential=%d ns parallel=%d ns streamLine=%d ns", sequentialDuration, parallelDuration, streamLineDuration)
            .isLessThanOrEqualTo((long) (parallelDuration * 1.10));
        assertThat(streamLineDuration)
            .withFailMessage("Expected StreamLine to beat java sequential stream for the blocking workload, but sequential=%d ns parallel=%d ns streamLine=%d ns", sequentialDuration, parallelDuration, streamLineDuration)
            .isLessThan(sequentialDuration);
    }

    @Test
    void shouldReduceScalarTerminalOverheadWhenUsingFusedCount() {
        final long materializedDuration = medianNanos(StreamLinePerformanceTest::runMaterializedCountWorkload);
        final long fusedDuration = medianNanos(StreamLinePerformanceTest::runFusedCountWorkload);

        assertThat(fusedDuration)
            .withFailMessage("Expected fused count to beat the materialized equivalent, but materialized=%d ns fused=%d ns", materializedDuration, fusedDuration)
            .isLessThan(materializedDuration);
    }

    private static void runUnlimitedPerItemWorkload() {
        assertThat(StreamLine.range(0, LIGHTWEIGHT_SIZE)
            .threads(-1)
            .chunks(1)
            .map(value -> value + 1)
            .toList()).hasSize(LIGHTWEIGHT_SIZE);
    }

    private static void runUnlimitedAutoChunkWorkload() {
        assertThat(StreamLine.range(0, LIGHTWEIGHT_SIZE)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + 1)
            .toList()).hasSize(LIGHTWEIGHT_SIZE);
    }

    private static void runUnlimitedExplicitChunkWorkload() {
        assertThat(StreamLine.range(0, LIGHTWEIGHT_SIZE)
            .threads(-1)
            .chunks(64)
            .map(value -> value + 1)
            .toList()).hasSize(LIGHTWEIGHT_SIZE);
    }

    private static void runBlockingWorkload(final int chunkSize) {
        assertThat(StreamLine.range(0, BLOCKING_SIZE)
            .threads(8)
            .chunks(chunkSize)
            .map(StreamLinePerformanceTest::blockingOperation)
            .toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runMaterializedCountWorkload() {
        assertThat(StreamLine.range(0, LIGHTWEIGHT_SIZE)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .toList()).hasSize(LIGHTWEIGHT_SIZE / 2);
    }

    private static void runFusedCountWorkload() {
        assertThat(StreamLine.range(0, LIGHTWEIGHT_SIZE)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .count()).isEqualTo(LIGHTWEIGHT_SIZE / 2);
    }

    private static void runSequentialBlockingWorkload() {
        assertThat(IntStream.range(0, BLOCKING_SIZE)
            .map(StreamLinePerformanceTest::blockingOperation)
            .boxed()
            .toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runParallelBlockingWorkload() {
        assertThat(IntStream.range(0, BLOCKING_SIZE)
            .parallel()
            .map(StreamLinePerformanceTest::blockingOperation)
            .boxed()
            .toList()).hasSize(BLOCKING_SIZE);
    }

    private static void runStreamLineBlockingWorkload() {
        assertThat(StreamLine.range(0, BLOCKING_SIZE)
            .threads(-1)
            .chunks(-1)
            .map(StreamLinePerformanceTest::blockingOperation)
            .toList()).hasSize(BLOCKING_SIZE);
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
}
