package berlin.yuna.streamline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Isolated
@Execution(ExecutionMode.SAME_THREAD)
class StreamLinePerformanceTest {

    private static final int WARMUP_RUNS = 2;
    private static final int MEASURED_RUNS = 7;
    private static final int TIMING_ATTEMPTS = 2;
    private static final int LIGHTWEIGHT_SIZE = 30_000;
    private static final int BLOCKING_SIZE = 4_000;

    @Test
    void shouldReduceSchedulingOverheadWhenChunkingUnlimitedWork() {
        assertTimingAdvantage(
            "automatic chunking should beat one task per item",
            StreamLinePerformanceTest::runUnlimitedPerItemWorkload,
            StreamLinePerformanceTest::runUnlimitedAutoChunkWorkload,
            1
        );
        assertTimingAdvantage(
            "explicit chunking should beat one task per item",
            StreamLinePerformanceTest::runUnlimitedPerItemWorkload,
            StreamLinePerformanceTest::runUnlimitedExplicitChunkWorkload,
            1
        );
    }

    @Test
    void shouldStayCompetitiveForBlockingWorkloadsWhenChunkingBoundedWorkers() {
        assertTimingAdvantage(
            "bounded chunking should stay competitive with per-item scheduling",
            () -> runBlockingWorkload(1),
            () -> runBlockingWorkload(16),
            1.25
        );
    }

    @Test
    void shouldCompareBlockingWorkloadAgainstJavaStreamsAndStreamLine() {
        assertTimingAdvantage(
            "parallel stream should beat sequential stream for blocking work",
            StreamLinePerformanceTest::runSequentialBlockingWorkload,
            StreamLinePerformanceTest::runParallelBlockingWorkload,
            1
        );
        assertTimingAdvantage(
            "StreamLine should stay competitive with java parallel stream for blocking work",
            StreamLinePerformanceTest::runParallelBlockingWorkload,
            StreamLinePerformanceTest::runStreamLineBlockingWorkload,
            1.25
        );
        assertTimingAdvantage(
            "StreamLine should beat sequential stream for blocking work",
            StreamLinePerformanceTest::runSequentialBlockingWorkload,
            StreamLinePerformanceTest::runStreamLineBlockingWorkload,
            1
        );
    }

    @Test
    void shouldReduceScalarTerminalOverheadWhenUsingFusedCount() {
        assertTimingAdvantage(
            "fused count should beat the materialized equivalent",
            StreamLinePerformanceTest::runMaterializedCountWorkload,
            StreamLinePerformanceTest::runFusedCountWorkload,
            1
        );
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

    private static void assertTimingAdvantage(final String reason, final Runnable baseline, final Runnable candidate, final double maxCandidateRatio) {
        final List<TimingComparison> comparisons = new ArrayList<>();
        for (int attempt = 0; attempt < TIMING_ATTEMPTS; attempt++) {
            final TimingComparison comparison = compareTimings(baseline, candidate);
            comparisons.add(comparison);
            if (comparison.isWithin(maxCandidateRatio)) {
                return;
            }
        }

        fail(
            "Expected %s with candidate <= %.2fx baseline in at least one stabilized attempt:%n%s",
            reason,
            maxCandidateRatio,
            timingReport(comparisons)
        );
    }

    private static TimingComparison compareTimings(final Runnable baseline, final Runnable candidate) {
        warmUp(baseline);
        warmUp(candidate);

        final long[] baselineDurations = new long[MEASURED_RUNS];
        final long[] candidateDurations = new long[MEASURED_RUNS];
        for (int run = 0; run < MEASURED_RUNS; run++) {
            if (run % 2 == 0) {
                baselineDurations[run] = measureNanos(baseline);
                candidateDurations[run] = measureNanos(candidate);
            } else {
                candidateDurations[run] = measureNanos(candidate);
                baselineDurations[run] = measureNanos(baseline);
            }
        }

        return new TimingComparison(medianNanos(baselineDurations), medianNanos(candidateDurations));
    }

    private static void warmUp(final Runnable workload) {
        for (int run = 0; run < WARMUP_RUNS; run++) {
            workload.run();
        }
    }

    private static long measureNanos(final Runnable workload) {
        final long start = System.nanoTime();
        workload.run();
        return System.nanoTime() - start;
    }

    private static long medianNanos(final long[] durations) {
        Arrays.sort(durations);
        return durations[MEASURED_RUNS / 2];
    }

    private static String timingReport(final List<TimingComparison> comparisons) {
        final StringBuilder result = new StringBuilder();
        for (int index = 0; index < comparisons.size(); index++) {
            final TimingComparison comparison = comparisons.get(index);
            result.append("attempt ")
                .append(index + 1)
                .append(": baseline=")
                .append(comparison.baselineNanos())
                .append(" ns candidate=")
                .append(comparison.candidateNanos())
                .append(" ns ratio=")
                .append(comparison.ratio())
                .append(System.lineSeparator());
        }
        return result.toString();
    }

    private record TimingComparison(long baselineNanos, long candidateNanos) {

        private boolean isWithin(final double maxCandidateRatio) {
            return candidateNanos <= (long) (baselineNanos * maxCandidateRatio);
        }

        private double ratio() {
            return baselineNanos == 0 ? Double.POSITIVE_INFINITY : (double) candidateNanos / baselineNanos;
        }
    }
}
