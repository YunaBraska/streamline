package berlin.yuna.streamline.model;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.CONCURRENT)
class StreamLineIsolationTest {

    private static final AtomicInteger OFFSET = new AtomicInteger();

    @RepeatedTest(64)
    void shouldKeepIndependentPipelinesIsolatedDuringConcurrentJUnitExecution() {
        final int offset = OFFSET.incrementAndGet() * 100;
        final List<Integer> expected = IntStream.range(0, 32)
            .map(value -> value + offset)
            .filter(value -> value % 2 == 0)
            .boxed()
            .toList();

        final List<Integer> collected = StreamLine.range(0, 32)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + offset)
            .filter(value -> value % 2 == 0)
            .collect(Collectors.toList());

        final long count = StreamLine.range(0, 32)
            .threads(-1)
            .chunks(-1)
            .map(value -> value + offset)
            .filter(value -> value % 2 == 0)
            .count();

        assertThat(collected).containsExactlyElementsOf(expected);
        assertThat(count).isEqualTo(expected.size());
    }
}
