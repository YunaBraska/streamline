package berlin.yuna.streamline.model;

import org.junit.jupiter.api.RepeatedTest;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamLineTest {

    // Ensure concurrency safety
    public static final int TEST_REPEAT = 256;

    @RepeatedTest(TEST_REPEAT)
    void testAdvanceCases() {
        assertThat(StreamLine.of(1, 2, 3, 4, 5)
            .map(x -> x + 1)
            .map(String::valueOf)
            .map(Integer::valueOf)
            .filter(x -> x > 3)
            .findFirst()).contains(4);

        assertThat(StreamLine.of(1, 2, 3, 4, 5)
            .map(x -> x + 1)
            .map(String::valueOf)
            .map(Integer::valueOf)
            .filter(x -> x > 3)
            .collect(Collectors.toList())).containsExactly(4, 5, 6);

        assertThat(StreamLine.of(1, 2, 3, 4, 5)
            .map(x -> x + 1)
            .map(String::valueOf)
            .map(Integer::valueOf)
            .filter(x -> x > 3)
            .toList()).containsExactly(4, 5, 6);
    }

    @RepeatedTest(TEST_REPEAT)
    void testMappings() {
        assertThat(Stream.of(1, 2, 3, 4, 5).mapToInt(x -> x + 1)).containsExactly(2, 3, 4, 5, 6);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).mapToInt(x -> x + 1)).containsExactly(2, 3, 4, 5, 6);

        assertThat(Stream.of(1, 2, 3, 4, 5).mapToLong(x -> x + 1)).containsExactly(2L, 3L, 4L, 5L, 6L);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).mapToLong(x -> x + 1)).containsExactly(2L, 3L, 4L, 5L, 6L);

        assertThat(Stream.of(1, 2, 3, 4, 5).mapToDouble(x -> x + 1)).containsExactly(2d, 3d, 4d, 5d, 6d);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).mapToDouble(x -> x + 1)).containsExactly(2d, 3d, 4d, 5d, 6d);

        assertThat(Stream.of(List.of(1, 2, 3, 4, 5)).flatMap(Collection::stream)).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(List.of(1, 2, 3, 4, 5)).flatMap(Collection::stream)).containsExactly(1, 2, 3, 4, 5);

        assertThat(Stream.of(1, 2, 3, 4, 5).flatMapToInt(IntStream::of)).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).flatMapToInt(IntStream::of)).containsExactly(1, 2, 3, 4, 5);

        assertThat(Stream.of(1, 2, 3, 4, 5).flatMapToLong(LongStream::of)).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).flatMapToLong(LongStream::of)).containsExactly(1L, 2L, 3L, 4L, 5L);

        assertThat(Stream.of(1, 2, 3, 4, 5).flatMapToDouble(DoubleStream::of)).containsExactly(1d, 2d, 3d, 4d, 5d);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).flatMapToDouble(DoubleStream::of)).containsExactly(1d, 2d, 3d, 4d, 5d);
    }

    @RepeatedTest(TEST_REPEAT)
    void testDistinct() {
        assertThat(Stream.of("AA", "AA", "BB", "BB", "CC").distinct().toList()).containsExactly("AA", "BB", "CC");
        assertThat(StreamLine.of("AA", "AA", "BB", "BB", "CC").distinct().toList()).containsExactly("AA", "BB", "CC");
    }

    @RepeatedTest(TEST_REPEAT)
    void testSort() {
        assertThat(Stream.of("CC", "AA", "BB").sorted().toList()).containsExactly("AA", "BB", "CC");
        assertThat(StreamLine.of("CC", "AA", "BB").sorted().toList()).containsExactly("AA", "BB", "CC");
    }

    @RepeatedTest(TEST_REPEAT)
    void testPeek() {
        final CopyOnWriteArrayList<String> peeks = new CopyOnWriteArrayList<>();
        assertThat(Stream.of("AA", "BB", "CC").sorted().peek(peeks::add).toList()).containsExactly("AA", "BB", "CC");
        assertThat(peeks).contains("AA", "BB", "CC");
        assertThat(StreamLine.of("AA", "BB", "CC").sorted().peek(peeks::add).toList()).containsExactly("AA", "BB", "CC");
        assertThat(peeks).contains("AA", "BB", "CC", "AA", "BB", "CC");
    }

    @RepeatedTest(TEST_REPEAT)
    void testSkipAndLimit() {
        assertThat(Stream.of("AA", "BB", "CC", "DD", "EE", "FF").skip(2).limit(2).toList()).containsExactly("CC", "DD");
        assertThat(StreamLine.of("AA", "BB", "CC", "DD", "EE", "FF").skip(2).limit(2).toList()).containsExactly("CC", "DD");
    }

    @RepeatedTest(TEST_REPEAT)
    void testRange() {
        assertThat(IntStream.range(2, 8).boxed().toList()).containsExactly(2, 3, 4, 5, 6, 7);
        assertThat(StreamLine.range(2, 8).toList()).containsExactly(2, 3, 4, 5, 6, 7);
        assertThat(StreamLine.range(Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("virtual-thread-", 0).factory()), 2, 8).toList()).containsExactly(2, 3, 4, 5, 6, 7);
        assertThat(StreamLine.range(8).toList()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @RepeatedTest(TEST_REPEAT)
    void testFindAny() {
        assertThat(Stream.of(1, 2, 3, 4, 5).findAny().get()).isPositive().isLessThan(6);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).findAny().get()).isPositive().isLessThan(6);
    }

    @RepeatedTest(TEST_REPEAT)
    void testIterator() {
        assertThat(Stream.of(1, 2, 3, 4, 5).iterator().next()).isEqualTo(1);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).iterator().next()).isEqualTo(1);
    }

    @RepeatedTest(TEST_REPEAT)
    void testSpliterator() {
        assertThat(Stream.of(1, 2, 3, 4, 5).spliterator().characteristics()).isEqualTo(17488);
        // StreamLine is consistent and independent of the source value
        assertThat(StreamLine.of(1, 2, 3, 4, 5).spliterator().characteristics()).isEqualTo(16464);
    }

    @RepeatedTest(TEST_REPEAT)
    void testCloseStream() {
        final AtomicInteger count = new AtomicInteger(0);
        final Stream<Integer> closeStream1 = Stream.of(1, 2, 3, 4, 5).onClose(count::incrementAndGet);
        final Stream<Integer> closeStream2 = StreamLine.of(1, 2, 3, 4, 5).onClose(count::incrementAndGet);
        assertThat(count).hasValue(0);
        closeStream1.close();
        assertThat(count).hasValue(1);
        closeStream2.close();
        assertThat(count).hasValue(2);
    }

    @RepeatedTest(TEST_REPEAT)
    void testUnorderedProcessing() {
        assertThat(Stream.of(1, 2, 3, 4, 5).unordered().toList()).contains(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).unordered().toList()).contains(1, 2, 3, 4, 5);
    }

    @RepeatedTest(TEST_REPEAT)
    void testSequentialProcessing() {
        assertThat(Stream.of(1, 2, 3, 4, 5).sequential().isParallel()).isFalse();
        assertThat(StreamLine.of(1, 2, 3, 4, 5).sequential().isParallel()).isFalse();

        assertThat(Stream.of(1, 2, 3, 4, 5).sequential().toList()).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).sequential().toList()).containsExactly(1, 2, 3, 4, 5);
    }

    @RepeatedTest(TEST_REPEAT)
    void testParallelProcessing() {
        assertThat(Stream.of(1, 2, 3, 4, 5).parallel().isParallel()).isTrue();
        assertThat(StreamLine.of(1, 2, 3, 4, 5).parallel().isParallel()).isTrue();

        assertThat(Stream.of(1, 2, 3, 4, 5).parallel().toList()).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).parallel().toList()).containsExactly(1, 2, 3, 4, 5);
    }

    @RepeatedTest(TEST_REPEAT)
    void testForEach() {
        final CopyOnWriteArrayList<String> peeks = new CopyOnWriteArrayList<>();
        Stream.of(1, 2, 3, 4, 5).forEach(integer -> peeks.add(integer.toString()));
        assertThat(peeks).contains("1", "2", "3", "4", "5");
        peeks.clear();
        StreamLine.of(1, 2, 3, 4, 5).forEach(integer -> peeks.add(integer.toString()));
        assertThat(peeks).contains("1", "2", "3", "4", "5");
        peeks.clear();
        StreamLine.of(1, 2, 3, 4, 5).forEachSync(integer -> peeks.add(integer.toString()));
        assertThat(peeks).contains("1", "2", "3", "4", "5");
        peeks.clear();
        Stream.of(1, 2, 3, 4, 5).forEachOrdered(integer -> peeks.add(integer.toString()));
        assertThat(peeks).containsExactly("1", "2", "3", "4", "5");
        peeks.clear();
        StreamLine.of(1, 2, 3, 4, 5).forEachOrdered(integer -> peeks.add(integer.toString()));
        assertThat(peeks).containsExactly("1", "2", "3", "4", "5");
    }

    @RepeatedTest(TEST_REPEAT)
    void testToArray() {
        assertThat(Stream.of(1, 2, 3, 4, 5).toArray()).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).toArray()).containsExactly(1, 2, 3, 4, 5);

        assertThat(Stream.of(1, 2, 3, 4, 5).toArray(Integer[]::new)).containsExactly(1, 2, 3, 4, 5);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).toArray(Integer[]::new)).containsExactly(1, 2, 3, 4, 5);
    }

    @RepeatedTest(TEST_REPEAT)
    void testReduce() {
        assertThat(Stream.of(1, 2, 3, 4, 5).reduce(Integer::sum)).contains(15);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).reduce(Integer::sum)).contains(15);

        assertThat(Stream.of(1, 2, 3, 4, 5).reduce(2, Integer::sum)).isEqualTo(17);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).reduce(2, Integer::sum)).isEqualTo(17);

        assertThat(Stream.of(3, 1, 4, 1, 5).reduce(Integer::max)).contains(5);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).reduce(Integer::max)).contains(5);
    }

    @RepeatedTest(TEST_REPEAT)
    void testMathFunctions() {
        assertThat(Stream.of(3, 1, 4, 1, 5).count()).isEqualTo(5);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).count()).isEqualTo(5);

        assertThat(Stream.of(3, 1, 4, 1, 5).max(Integer::compareTo)).contains(5);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).max(Integer::compareTo)).contains(5);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).max().orElse(-1)).isEqualTo(5);

        assertThat(Stream.of(3, 1, 4, 1, 5).min(Integer::compareTo)).contains(1);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).min(Integer::compareTo)).contains(1);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).min().orElse(-1)).isEqualTo(1);

        assertThat(StreamLine.of(3, 1, 4, 1, 5).average().orElse(-1)).isEqualTo(2.8);
        assertThat(StreamLine.of(3, 1, 4, 1, 5).statistics()).isNotNull();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).sum()).isEqualTo(14);
        assertThat(StreamLine.of("1", "2", "3", "4", "5").average()).isEmpty();
        assertThat(StreamLine.of("1", "2", "3", "4", "5").statistics()).isNotNull();
        assertThat(StreamLine.of("1", "2", "3", "4", "5").sum()).isZero();
    }

    @RepeatedTest(TEST_REPEAT)
    void testAllMatch() {
        assertThat(Stream.of(3, 1, 4, 1, 5).allMatch(integer -> integer == 1)).isFalse();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).allMatch(integer -> integer == 1)).isFalse();
        assertThat(Stream.of(3, 1, 4, 1, 5).allMatch(integer -> integer > 0 && integer < 6)).isTrue();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).allMatch(integer -> integer > 0 && integer < 6)).isTrue();
    }

    @RepeatedTest(TEST_REPEAT)
    void testAnyMatch() {
        assertThat(Stream.of(3, 1, 4, 1, 5).anyMatch(integer -> integer == 1)).isTrue();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).anyMatch(integer -> integer == 1)).isTrue();
        assertThat(Stream.of(3, 1, 4, 1, 5).anyMatch(integer -> integer == 6)).isFalse();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).anyMatch(integer -> integer == 6)).isFalse();
    }

    @RepeatedTest(TEST_REPEAT)
    void testNoneMatch() {
        assertThat(Stream.of(3, 1, 4, 1, 5).noneMatch(integer -> integer == 1)).isFalse();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).noneMatch(integer -> integer == 1)).isFalse();
        assertThat(Stream.of(3, 1, 4, 1, 5).noneMatch(integer -> integer == 6)).isTrue();
        assertThat(StreamLine.of(3, 1, 4, 1, 5).noneMatch(integer -> integer == 6)).isTrue();
    }

    @RepeatedTest(TEST_REPEAT)
    void testException() {
        assertThatThrownBy(() -> Stream.of(3, 1, 4, 1, 5).noneMatch(integer -> {
            throw new RuntimeException("Test Error");
        })).isInstanceOf(RuntimeException.class).hasMessage("Test Error");
        assertThatThrownBy(() -> StreamLine.of(3, 1, 4, 1, 5).noneMatch(integer -> {
            throw new RuntimeException("Test Error");
        })).isInstanceOf(RuntimeException.class).hasMessage("Test Error");
    }

    @RepeatedTest(TEST_REPEAT)
    void testReduceWithIdentityAndAccumulator() {
        assertThat(Stream.of(1, 2, 3, 4, 5).reduce(0, (a, b) -> a + b, Integer::sum)).isEqualTo(15);
        assertThat(StreamLine.of(1, 2, 3, 4, 5).reduce(0, (a, b) -> a + b, Integer::sum)).isEqualTo(15);

        assertThat(Stream.of("a", "b", "c").reduce("", (a, b) -> a + b, String::concat)).isEqualTo("abc");
        assertThat(StreamLine.of("a", "b", "c").reduce("", (a, b) -> a + b, String::concat)).isEqualTo("abc");
    }

    @RepeatedTest(TEST_REPEAT)
    void testCollectToList() {
        assertThat((List<?>) Stream.of(1, 2, 3, 4, 5).collect(ArrayList::new, List::add, List::addAll)).isEqualTo(List.of(1, 2, 3, 4, 5));
        assertThat((List<?>) StreamLine.of(1, 2, 3, 4, 5).collect(ArrayList::new, List::add, List::addAll)).isEqualTo(List.of(1, 2, 3, 4, 5));
    }

    @RepeatedTest(TEST_REPEAT)
    void testCollectGrouping() {
        assertThat(Stream.of(3, 1, 4, 1, 5).collect(Collectors.groupingBy(integer -> integer == 1)))
            .contains(new AbstractMap.SimpleEntry<>(true, List.of(1, 1)), new AbstractMap.SimpleEntry<>(false, List.of(3, 4, 5)));
        assertThat(StreamLine.of(3, 1, 4, 1, 5).collect(Collectors.groupingBy(integer -> integer == 1)))
            .contains(new AbstractMap.SimpleEntry<>(true, List.of(1, 1)), new AbstractMap.SimpleEntry<>(false, List.of(3, 4, 5)));
    }


    @RepeatedTest(TEST_REPEAT)
    void TestAccessors() {
        final StreamLine<Integer> stream = StreamLine.of(1, 2, 3, 4, 5);
        assertThat(stream.ordered()).isTrue();
        assertThat(stream.ordered(false).ordered()).isFalse();
        assertThat(stream.threads(1).threads()).isEqualTo(1);
        assertThat(stream.threads(2).threads()).isEqualTo(2);
        assertThat(stream.executor()).isNotNull();
    }
}
