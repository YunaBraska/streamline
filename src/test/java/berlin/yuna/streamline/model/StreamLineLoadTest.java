package berlin.yuna.streamline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class StreamLineLoadTest {

    @Test
    void shouldCompleteConcurrentLoadAcrossThreadAndChunkConfigurations() throws Exception {
        final ExecutorService loadExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("load-test-", 0).factory());
        try {
            final List<Callable<List<Integer>>> tasks = new ArrayList<>();
            for (final int threads : new int[]{1, 2, 4, -1}) {
                for (final int chunks : new int[]{1, 2, 8, -1}) {
                    tasks.add(() -> executeLoad(threads, chunks));
                    tasks.add(() -> executeLoad(threads, chunks));
                }
            }

            final List<Future<List<Integer>>> futures = loadExecutor.invokeAll(tasks);
            final List<Integer> expected = IntStream.range(0, 256).map(value -> value + 1).boxed().toList();
            for (final Future<List<Integer>> future : futures) {
                assertThat(future.get(10, TimeUnit.SECONDS)).containsExactlyElementsOf(expected);
            }
        } finally {
            loadExecutor.shutdown();
            assertThat(loadExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static List<Integer> executeLoad(final int threads, final int chunks) {
        return StreamLine.range(0, 256)
            .threads(threads)
            .chunks(chunks)
            .map(StreamLineLoadTest::loadOperation)
            .toList();
    }

    private static int loadOperation(final int value) {
        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
        return value + 1;
    }
}
