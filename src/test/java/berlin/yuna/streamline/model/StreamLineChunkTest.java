package berlin.yuna.streamline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamLineChunkTest {

    @Test
    void shouldNormalizeThreadAndChunkValues() {
        final StreamLine<Integer> stream = StreamLine.of(1, 2, 3);

        assertThat(stream.threads(-7).threads()).isEqualTo(-1);
        assertThat(stream.chunks(-5).chunks()).isEqualTo(-1);
        assertThat(stream.threads(0).threads()).isEqualTo(1);
        assertThat(stream.chunks(0).chunks()).isEqualTo(1);
    }

    @Test
    void shouldRespectConfiguredThreadLimitWhenChunking() {
        final AtomicInteger activeWorkers = new AtomicInteger();
        final AtomicInteger maxActiveWorkers = new AtomicInteger();

        final List<Integer> result = StreamLine.range(0, 24)
            .threads(2)
            .chunks(3)
            .map(value -> {
                final int currentWorkers = activeWorkers.incrementAndGet();
                maxActiveWorkers.accumulateAndGet(currentWorkers, Math::max);
                try {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                    return value;
                } finally {
                    activeWorkers.decrementAndGet();
                }
            })
            .toList();

        assertThat(result).containsExactlyElementsOf(IntStream.range(0, 24).boxed().toList());
        assertThat(maxActiveWorkers.get()).isEqualTo(2);
    }

    @Test
    void shouldUseOneWorkerPerChunkWhenThreadsAreUnlimited() throws Exception {
        final Set<String> workerNames = ConcurrentHashMap.newKeySet();
        final ExecutorService executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("chunk-worker-", 0).factory());
        try {
            final List<Integer> result = StreamLine.range(executor, 0, 10)
                .threads(-9)
                .chunks(3)
                .map(value -> {
                    workerNames.add(Thread.currentThread().getName());
                    return value;
                })
                .toList();

            assertThat(result).containsExactlyElementsOf(IntStream.range(0, 10).boxed().toList());
            assertThat(workerNames).hasSize(4);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldCollectWithoutReexecutingThePipeline() {
        final AtomicInteger invocations = new AtomicInteger();

        final List<Integer> result = StreamLine.of(1, 2, 3)
            .map(value -> {
                invocations.incrementAndGet();
                return value * 2;
            })
            .collect(Collectors.toList());

        assertThat(result).containsExactly(2, 4, 6);
        assertThat(invocations).hasValue(3);
    }

    @Test
    void shouldPreserveEncounterOrderWhenCollectingChunkLocals() {
        final List<Integer> result = StreamLine.range(0, 12)
            .threads(4)
            .chunks(2)
            .map(value -> value + 1)
            .collect(ArrayList::new, List::add, List::addAll);

        assertThat(result).containsExactlyElementsOf(IntStream.rangeClosed(1, 12).boxed().toList());
    }

    @Test
    void shouldSupportFusedScalarTerminalsAcrossChunkedWorkers() {
        final long count = StreamLine.range(0, 32)
            .threads(4)
            .chunks(3)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .count();

        final DoubleSummaryStatistics statistics = StreamLine.range(0, 32)
            .threads(4)
            .chunks(3)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .statistics();

        assertThat(count).isEqualTo(16);
        assertThat(statistics.getCount()).isEqualTo(16);
        assertThat(statistics.getSum()).isEqualTo(272);
        assertThat(statistics.getAverage()).isEqualTo(17);
    }

    @Test
    void shouldHandleEmptyResultsForFusedTerminals() {
        final List<Integer> emptyCollected = StreamLine.<Integer>of().threads(4).chunks(3).collect(ArrayList::new, List::add, List::addAll);

        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).count()).isZero();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).sum()).isZero();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).average()).isEmpty();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).statistics().getCount()).isZero();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).collect(Collectors.toList())).isEmpty();
        assertThat(emptyCollected).isEmpty();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).findFirst()).isEmpty();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).findAny()).isEmpty();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).anyMatch(value -> true)).isFalse();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).allMatch(value -> false)).isTrue();
        assertThat(StreamLine.<Integer>of().threads(4).chunks(3).noneMatch(value -> true)).isTrue();
    }

    @Test
    void shouldPropagateExceptionsFromFusedOperations() {
        assertThatThrownBy(() -> StreamLine.range(0, 12)
            .threads(4)
            .chunks(3)
            .map(value -> {
                if (value == 5) {
                    throw new IllegalArgumentException("map failure");
                }
                return value;
            })
            .count())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("map failure");

        assertThatThrownBy(() -> StreamLine.range(0, 12)
            .threads(4)
            .chunks(3)
            .filter(value -> {
                if (value == 7) {
                    throw new IllegalStateException("filter failure");
                }
                return true;
            })
            .statistics())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("filter failure");
    }

    @Test
    void shouldPropagateExceptionsFromFusedCollectors() {
        assertThatThrownBy(() -> StreamLine.range(0, 12)
            .threads(4)
            .chunks(3)
            .collect(ArrayList::new, (result, value) -> {
                if (value == 5) {
                    throw new IllegalArgumentException("accumulator failure");
                }
                result.add(value);
            }, List::addAll))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("accumulator failure");

        assertThatThrownBy(() -> StreamLine.range(0, 12)
            .threads(4)
            .chunks(3)
            .collect(ConcurrentHashMap<Integer, Integer>::new, (result, value) -> result.put(value, value), (left, right) -> {
                if (!right.isEmpty()) {
                    throw new IllegalStateException("combiner failure");
                }
            }))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("combiner failure");
    }

    @Test
    void shouldPreserveFusedTerminalResultsAcrossRepeatedInvocations() {
        final StreamLine<Integer> stream = StreamLine.range(0, 24)
            .threads(4)
            .chunks(3)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0);

        final List<Integer> expected = IntStream.range(0, 24)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .boxed()
            .toList();

        assertThat(stream.count()).isEqualTo(expected.size());
        assertThat(stream.count()).isEqualTo(expected.size());
        assertThat(stream.collect(Collectors.toList())).containsExactlyElementsOf(expected);
        assertThat(stream.collect(Collectors.toList())).containsExactlyElementsOf(expected);
        assertThat(stream.statistics().getSum()).isEqualTo(expected.stream().mapToInt(Integer::intValue).sum());
        assertThat(stream.statistics().getSum()).isEqualTo(expected.stream().mapToInt(Integer::intValue).sum());
    }

    @Test
    void shouldPreserveChunkConfigurationAcrossDerivedStreams() {
        final StreamLine<Integer> stream = (StreamLine<Integer>) StreamLine.of(5, 4, 3, 2, 1)
            .threads(3)
            .chunks(2)
            .sorted();

        assertThat(stream.threads()).isEqualTo(3);
        assertThat(stream.chunks()).isEqualTo(2);
        assertThat(stream.toList()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void shouldAllowSubclassesToPreserveDerivedType() {
        final TrackingStreamLine<Integer> stream = new TrackingStreamLine<>(null, 5, 4, 3, 2, 1);

        final StreamLine<Integer> derived = stream
            .threads(3)
            .chunks(2)
            .sorted();

        assertThat(derived).isInstanceOf(TrackingStreamLine.class);
        assertThat(((TrackingStreamLine<Integer>) derived).marker()).isEqualTo("tracking");
        assertThat(derived.threads()).isEqualTo(3);
        assertThat(derived.chunks()).isEqualTo(2);
        assertThat(derived.toList()).containsExactly(1, 2, 3, 4, 5);
    }

    @ParameterizedTest(name = "default executor threads={0} chunks={1}")
    @MethodSource("configurationArguments")
    void shouldSupportAllThreadAndChunkCombinationsWithDefaultExecutor(final int threads, final int chunks) {
        assertConfigurationMatrix(null, threads, chunks);
    }

    @ParameterizedTest(name = "custom executor threads={0} chunks={1}")
    @MethodSource("configurationArguments")
    void shouldSupportAllThreadAndChunkCombinationsWithCustomExecutor(final int threads, final int chunks) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            assertConfigurationMatrix(executor, threads, chunks);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest(name = "fused terminals default executor threads={0} chunks={1}")
    @MethodSource("configurationArguments")
    void shouldSupportFusedTerminalMatrixWithDefaultExecutor(final int threads, final int chunks) {
        assertFusedTerminalMatrix(null, threads, chunks);
    }

    @ParameterizedTest(name = "fused terminals custom executor threads={0} chunks={1}")
    @MethodSource("configurationArguments")
    void shouldSupportFusedTerminalMatrixWithCustomExecutor(final int threads, final int chunks) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            assertFusedTerminalMatrix(executor, threads, chunks);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest(name = "indexed foreach threads={0} chunks={1}")
    @MethodSource("configurationArguments")
    void shouldExposeIndexesAcrossAllThreadAndChunkCombinations(final int threads, final int chunks) {
        final List<String> syncIndexValues = new ArrayList<>();
        StreamLine.of(9, 7, 5, 3, 1)
            .threads(threads)
            .chunks(chunks)
            .forEachSync((index, value) -> syncIndexValues.add(index + ":" + value));
        assertThat(syncIndexValues).containsExactly("0:9", "1:7", "2:5", "3:3", "4:1");

        final List<String> orderedIndexValues = new ArrayList<>();
        ((StreamLine<Integer>) StreamLine.of(5, 4, 3, 2, 1)
            .threads(threads)
            .chunks(chunks)
            .sorted())
            .forEachOrdered((index, value) -> orderedIndexValues.add(index + ":" + value));
        assertThat(orderedIndexValues).containsExactly("0:1", "1:2", "2:3", "3:4", "4:5");

        final ConcurrentHashMap<Integer, Integer> asyncIndexValues = new ConcurrentHashMap<>();
        StreamLine.range(0, 12)
            .threads(threads)
            .chunks(chunks)
            .unordered()
            .map(value -> value * 2)
            .forEach((index, value) -> asyncIndexValues.put(index, value));

        assertThat(asyncIndexValues.keySet()).containsExactlyInAnyOrderElementsOf(IntStream.range(0, 12).boxed().toList());
        assertThat(asyncIndexValues.values()).containsExactlyInAnyOrderElementsOf(IntStream.range(0, 12).map(value -> value * 2).boxed().toList());
    }

    private void assertConfigurationMatrix(final ExecutorService executor, final int threads, final int chunks) {
        final List<Integer> expected = IntStream.range(0, 24)
            .map(value -> value * 3)
            .filter(value -> value % 2 == 0)
            .boxed()
            .toList();

        final List<Integer> ordered = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value * 3)
            .filter(value -> value % 2 == 0)
            .toList();
        assertThat(ordered).containsExactlyElementsOf(expected);

        final List<Integer> unordered = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value * 3)
            .filter(value -> value % 2 == 0)
            .unordered()
            .toList();
        assertThat(unordered).containsExactlyInAnyOrderElementsOf(expected);
    }

    private void assertFusedTerminalMatrix(final ExecutorService executor, final int threads, final int chunks) {
        final List<Integer> expected = IntStream.range(0, 24)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .boxed()
            .toList();
        final double expectedSum = expected.stream().mapToInt(Integer::intValue).sum();

        assertThat(createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .count()).isEqualTo(expected.size());

        final DoubleSummaryStatistics statistics = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .statistics();
        assertThat(statistics.getCount()).isEqualTo(expected.size());
        assertThat(statistics.getSum()).isEqualTo(expectedSum);
        assertThat(statistics.getMin()).isEqualTo(2);
        assertThat(statistics.getMax()).isEqualTo(24);

        final List<Integer> collected = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .collect(Collectors.toList());
        assertThat(collected).containsExactlyElementsOf(expected);

        final List<Integer> unorderedCollected = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value + 1)
            .filter(value -> value % 2 == 0)
            .unordered()
            .collect(ArrayList::new, List::add, List::addAll);
        assertThat(unorderedCollected).containsExactlyInAnyOrderElementsOf(expected);

        final Map<Boolean, List<Integer>> grouped = createRange(executor, 24)
            .threads(threads)
            .chunks(chunks)
            .map(value -> value + 1)
            .collect(Collectors.groupingBy(value -> value % 2 == 0));
        assertThat(grouped.get(true)).containsExactlyElementsOf(expected);
    }

    private static Stream<Arguments> configurationArguments() {
        final int[] threadValues = {1, 2, 4, -1};
        final int[] chunkValues = {1, 2, 8, -1};
        final List<Arguments> arguments = new ArrayList<>();
        for (final int threadValue : threadValues) {
            for (final int chunkValue : chunkValues) {
                arguments.add(Arguments.of(threadValue, chunkValue));
            }
        }
        return arguments.stream();
    }

    private StreamLine<Integer> createRange(final ExecutorService executor, final int endExclusive) {
        return executor == null ? StreamLine.range(0, endExclusive) : StreamLine.range(executor, 0, endExclusive);
    }

    private static final class TrackingStreamLine<T> extends StreamLine<T> {

        private TrackingStreamLine(final ExecutorService executor, final T... values) {
            super(executor, values);
        }

        private String marker() {
            return "tracking";
        }

        @Override
        protected <R> StreamLine<R> newStream(final R[] values) {
            return new TrackingStreamLine<>(executor(), values).ordered(ordered()).threads(threads()).chunks(chunks());
        }
    }
}
