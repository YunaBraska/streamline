package berlin.yuna.streamline.model;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

/**
 * {@link StreamLine} provides a simplified, high-performance Stream API utilizing exchangeable {@link ExecutorService},
 * with a focus on leveraging<code>Project Loom</code>'s virtual and non-blocking threads for efficient concurrent processing ({@link StreamLine#VIRTUAL_EXECUTOR}).
 * Unlike the default {@link Stream#parallel()}, {@link StreamLine} is optimized for multithreaded environments showcasing superior performance with a design that avoids the common pitfalls of resource management in stream operations.
 * <p><u><b>Usage Example:</b><u/>
 * <pre>
 * {@link StreamLine}.of("one", "two", "three")
 *           .threads(-1) // unlimited threads
 *           .forEach(System.out::println);
 * </pre>
 * </p><p><u><b>Performance and Scalability:</b><u/>
 * {@link StreamLine} outperforms standard Java {@link Stream} in concurrent scenarios:
 * Tested 10 Concurrent Streams with 10 Tasks each (10 Cores CPU).
 * <ul>
 *     <li>Loop [for]: 1.86s</li>
 *     <li>{@link Stream} [Sequential]: 1.29s</li>
 *     <li>{@link Stream}  [Parallel]: 724ms</li>
 *     <li>{@link StreamLine}  [Ordered]: 118ms</li>
 *     <li>{@link StreamLine}  [Unordered]: 109ms</li>
 *     <li>{@link StreamLine}  [2 Threads]: 500ms</li>
 * </ul>
 * </p><p><u><b>Additional Methods:</b><u/>
 * {@link StreamLine} comes with additional methods out of the box to avoid performance loss at {@link IntStream}, {@link LongStream} and {@link DoubleStream}
 * <ul>
 *     <li>{@link StreamLine#range(int, int)}: same as {@link IntStream#range(int, int)}</li>
 *     <li>{@link StreamLine#count()}: same as {@link IntStream#count()}</li>
 *     <li>{@link StreamLine#sum()}: same as {@link IntStream#sum()}</li>
 *     <li>{@link StreamLine#max()}: same as {@link IntStream#max()}</li>
 *     <li>{@link StreamLine#min()}: same as {@link IntStream#min()}</li>
 *     <li>{@link StreamLine#average()}: same as {@link IntStream#average()}</li>
 *     <li>{@link StreamLine#statistics()}: same as {@link IntStream#summaryStatistics()}</li>
 * </ul>
 * </p><p><u><b>Note on Concurrency:</b><u/>
 * Be mindful when handling functions like {@link Stream#forEach(Consumer)}; This function calls the consumer concurrently.
 * This behaviour is the same as in {@link Stream#parallel()} but its more noticeable due to the performance of {@link StreamLine}.
 * </p><p><u><b>Limitations:</b><u/>
 * The concurrent processing does not extend to operations returning type-specific streams like {@link IntStream}, {@link LongStream}, {@link DoubleStream}, {@link OptionalInt}, {@link OptionalLong}, {@link OptionalDouble}, etc.
 * {@link StreamLine} has more <a href="package-summary.html#StreamOps">Terminal operations</a> due its simple design
 * </p>
 *
 * @param <T> the type of the stream elements
 */
@SuppressWarnings({"unchecked", "java:S1905"}) // Suppress SonarLint warning about casting to T
public class StreamLine<T> implements Stream<T> {
    private final T[] source;
    private final ExecutorService executor;
    private final List<Function<Object, Object>> operations = new ArrayList<>();
    private boolean ordered = true;
    private int threads;
    private Runnable closeHandler;
    public static final ExecutorService VIRTUAL_EXECUTOR = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("virtual-thread-", 0).factory());

    /**
     * Constructs a stream with a custom executor and source elements.
     *
     * @param executor [Optional] The executor to handle parallel processing.
     * @param values   The source elements for the stream.
     */
    public StreamLine(final ExecutorService executor, final T... values) {
        this.source = values;
        this.executor = executor != null ? executor : VIRTUAL_EXECUTOR;
        threads = executor == null ? Math.max(2, Runtime.getRuntime().availableProcessors() / 2) : 10;
    }

    /**
     * Creates a {@link StreamLine}. from given elements.
     *
     * @param values The elements to include in the new stream.
     * @return A new {@link StreamLine}.
     */
    public static <T> StreamLine<T> of(final T... values) {
        return of(null, values);
    }

    /**
     * Creates a {@link StreamLine}. from given elements and a custom executor.
     *
     * @param executor [Optional] The executor for parallel processing.
     * @param values   The elements to include in the new stream.
     * @return A new {@link StreamLine}..
     */
    public static <T> StreamLine<T> of(final ExecutorService executor, final T... values) {
        return new StreamLine<>(executor, values);
    }

    /**
     * Creates a {@link StreamLine}. from a single element.
     *
     * @param value The single element to create the stream from.
     * @return A new {@link StreamLine}..
     */
    public static <T> StreamLine<T> of(final T value) {
        return of(null, (T[]) new Object[]{value});
    }

    /**
     * Creates a {@link StreamLine}. from a single element with a custom executor.
     *
     * @param executor [Optional] The executor for parallel processing.
     * @param value    The single element to create the stream from.
     * @return A new {@link StreamLine}..
     */
    public static <T> StreamLine<T> of(final ExecutorService executor, final T value) {
        return of(executor, (T[]) new Object[]{value});
    }

    /**
     * Creates a {@link StreamLine}. representing a range of integers.
     *
     * @param endExclusive The end (exclusive) of the range.
     * @return A new {@link StreamLine}..
     */
    public static StreamLine<Integer> range(final int endExclusive) {
        return range(null, 0, endExclusive);
    }

    /**
     * Creates a {@link StreamLine}. representing a range of integers.
     *
     * @param startInclusive The start (inclusive) of the range.
     * @param endExclusive   The end (exclusive) of the range.
     * @return A new {@link StreamLine}..
     */
    public static StreamLine<Integer> range(final int startInclusive, final int endExclusive) {
        return range(null, startInclusive, endExclusive);
    }

    /**
     * Creates a {@link StreamLine}. representing a range of integers with a custom executor.
     *
     * @param executor       [Optional] The executor for parallel processing.
     * @param startInclusive The start (inclusive) of the range.
     * @param endExclusive   The end (exclusive) of the range.
     * @return A new {@link StreamLine}..
     */
    public static StreamLine<Integer> range(final ExecutorService executor, final int startInclusive, final int endExclusive) {
        final List<Integer> range = new ArrayList<>();
        for (int i = (startInclusive > -1 ? startInclusive : 0); i < endExclusive; i++) {
            range.add(i);
        }
        return of(executor, range.toArray(Integer[]::new));
    }

    /**
     * Returns whether this stream is ordered.
     *
     * @return True if the stream is ordered, false otherwise.
     */
    public boolean ordered() {
        return ordered;
    }

    /**
     * Sets the ordered state of the stream.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param ordered Whether the stream should be ordered.
     * @return The current {@link StreamLine}..
     */
    public StreamLine<T> ordered(final boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    /**
     * Gets the limit of threads available for parallel processing. -1 all available executor threads.
     *
     * @return The number of threads.
     */
    public int threads() {
        return threads;
    }

    /**
     * Sets the limit of threads for parallel processing. -1 all available executor threads.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param threads The number of threads to use.
     * @return The current {@link StreamLine}..
     */
    public StreamLine<T> threads(final int threads) {
        this.threads = threads == 0 ? 1 : threads;
        return this;
    }

    /**
     * Returns the executor associated with this stream.
     *
     * @return The executor.
     */
    public ExecutorService executor() {
        return executor;
    }

    /**
     * Applies a transformation function to each element of the stream.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param mapper A function to apply to each element.
     * @return A stream consisting of the results of applying the given function.
     */
    @Override
    public <R> Stream<R> map(final Function<? super T, ? extends R> mapper) {
        operations.add((Function<Object, Object>) mapper);
        return (Stream<R>) this;
    }

    /**
     * Converts executes all operations and returns a new Stream with the results.
     * For Performance, it's recommended to use the usual {@link StreamLine#map(Function)} functions instead.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param mapper A function to convert elements to int values.
     * @return A new Stream from the {@link StreamLine} results.
     */
    @Override
    public IntStream mapToInt(final ToIntFunction<? super T> mapper) {
        operations.add(item -> mapper.applyAsInt((T) item));
        return (isParallel() ? Arrays.stream(executeTerminal()).parallel() : Arrays.stream(executeTerminal())).mapToInt(Integer.class::cast);
    }

    /**
     * Converts executes all operations and returns a new Stream with the results.
     * For Performance, it's recommended to use the usual {@link StreamLine#map(Function)} functions instead.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param mapper A function to convert elements to int values.
     * @return A new Stream from the {@link StreamLine} results.
     */
    @Override
    public LongStream mapToLong(final ToLongFunction<? super T> mapper) {
        operations.add(item -> mapper.applyAsLong((T) item));
        return (isParallel() ? Arrays.stream(executeTerminal()).parallel() : Arrays.stream(executeTerminal())).mapToLong(Long.class::cast);
    }

    /**
     * Converts executes all operations and returns a new Stream with the results.
     * For Performance, it's recommended to use the usual {@link StreamLine#map(Function)} functions instead.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param mapper A function to convert elements to int values.
     * @return A new Stream from the {@link StreamLine} results.
     */
    @Override
    public DoubleStream mapToDouble(final ToDoubleFunction<? super T> mapper) {
        operations.add(item -> mapper.applyAsDouble((T) item));
        return (isParallel() ? Arrays.stream(executeTerminal()).parallel() : Arrays.stream(executeTerminal())).mapToDouble(Double.class::cast);
    }

    /**
     * Transforms each element into zero or more elements by applying a function to each element.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * Differs from {@link Stream#map(Function)} which is <a href="package-summary.html#StreamOps">Intermediate operation</a></p>.
     * This closes the streams as soon as possible to keep things simple and continue with clean multi thread operations.
     *
     * @param mapper A function to apply to each element, which returns a stream of new values.
     * @return A new stream consisting of all elements produced by applying the function to each element.
     */
    @Override
    public <R> Stream<R> flatMap(final Function<? super T, ? extends Stream<? extends R>> mapper) {
        operations.add(item -> mapper.apply((T) item));
        return (Stream<R>) StreamLine.of(executor, Arrays.stream(executeTerminal()).flatMap(item -> (Stream<R>) item).toArray()).ordered(ordered).threads(threads);
    }

    /**
     * Converts elements of this stream to a {@link IntStream} by applying a function that produces a {@link IntStream} for each element.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * It's recommended to use the usual {@link StreamLine#map(Function)} as the performance of {@link StreamLine} ends here.
     *
     * @param mapper A function to apply to each element, which returns a {@link IntStream} of new values.
     * @return A new {@link IntStream} consisting of all long values produced by applying the function to each element.
     */
    @Override
    public IntStream flatMapToInt(final Function<? super T, ? extends IntStream> mapper) {
        operations.add(item -> mapper.apply((T) item));
        return IntStream.of(Arrays.stream(executeTerminal()).flatMapToInt(IntStream.class::cast).toArray());
    }

    /**
     * Converts elements of this stream to a {@link LongStream} by applying a function that produces a {@link LongStream} for each element.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * It's recommended to use the usual {@link StreamLine#map(Function)} as the performance of {@link StreamLine} ends here.
     *
     * @param mapper A function to apply to each element, which returns a {@link LongStream} of new values.
     * @return A new {@link LongStream} consisting of all long values produced by applying the function to each element.
     */
    @Override
    public LongStream flatMapToLong(final Function<? super T, ? extends LongStream> mapper) {
        operations.add(item -> mapper.apply((T) item));
        return LongStream.of(Arrays.stream(executeTerminal()).flatMapToLong(LongStream.class::cast).toArray());
    }

    /**
     * Converts elements of this stream to a {@link DoubleStream} by applying a function that produces a {@link DoubleStream} for each element.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * It's recommended to use the usual {@link StreamLine#map(Function)} as the performance of {@link StreamLine} ends here.
     *
     * @param mapper A function to apply to each element, which returns a {@link DoubleStream} of new values.
     * @return A new {@link DoubleStream} consisting of all long values produced by applying the function to each element.
     */
    @Override
    public DoubleStream flatMapToDouble(final Function<? super T, ? extends DoubleStream> mapper) {
        operations.add(item -> mapper.apply((T) item));
        return DoubleStream.of(Arrays.stream(executeTerminal()).flatMapToDouble(DoubleStream.class::cast).toArray());
    }

    /**
     * Returns a stream consisting of distinct elements (according to Object.equals(Object)).
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @return A stream consisting of the distinct elements of this stream.
     */
    @Override
    public Stream<T> distinct() {
        final Set<T> seen = ConcurrentHashMap.newKeySet();
        operations.add(item -> seen.add((T) item) ? item : null);
        return this;
    }

    /**
     * Returns a stream consisting of the elements of this stream, sorted according to natural order.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @return A stream consisting of the sorted elements of this stream.
     */
    @Override
    public Stream<T> sorted() {
        return sorted(null);
    }

    /**
     * Returns a stream consisting of the elements of this stream, sorted according to the provided Comparator.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param comparator A Comparator to be used to compare stream elements.
     * @return A new stream consisting of the sorted elements of this stream.
     */
    @Override
    public Stream<T> sorted(final Comparator<? super T> comparator) {
        final T[] values = executeTerminal();
        Arrays.sort(values, comparator);
        return StreamLine.of(executor, values).ordered(ordered).threads(threads);
    }

    /**
     * Returns a stream consisting of the elements of this stream, each modified by the given function.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param action A non-interfering action to perform on the elements as they are consumed from the stream.
     * @return A stream consisting of the elements after applying the given action.
     */
    @Override
    public Stream<T> peek(final Consumer<? super T> action) {
        operations.add(item -> {
            action.accept((T) item);
            return item;
        });
        return this;
    }

    /**
     * Returns a stream consisting of the elements of this stream, truncated to be no longer than maxSize in length.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param maxSize The maximum number of elements the stream should be limited to.
     * @return A new stream consisting of the elements of this stream, truncated to maxSize in length.
     */
    @Override
    public Stream<T> limit(final long maxSize) {
        final T[] values = executeTerminal();
        return maxSize < 1 || maxSize > values.length ? this : new StreamLine<>(executor, Arrays.copyOfRange(values, 0, (int) Math.min(maxSize, values.length))).ordered(ordered).threads(threads);
    }

    /**
     * Returns a stream consisting of the remaining elements of this stream after discarding the first n elements.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param n The number of leading elements to skip.
     * @return A new stream consisting of the remaining elements of this stream after skipping the first n elements.
     */
    @Override
    public Stream<T> skip(final long n) {
        final T[] values = executeTerminal();
        return n < 1 ? this : new StreamLine<>(executor, Arrays.copyOfRange(values, (int) Math.min(n, values.length), values.length)).ordered(ordered).threads(threads);
    }

    /**
     * Returns a stream consisting of the elements of this stream that match the given predicate.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param predicate A predicate to apply to each element to determine if it should be included.
     * @return A stream consisting of the elements of this stream that match the given predicate.
     */
    @Override
    public Stream<T> filter(final Predicate<? super T> predicate) {
        operations.add(item -> predicate.test((T) item) ? item : null);
        return this;
    }

    /**
     * Returns an Optional describing the first element of this stream, or an empty Optional if the stream is empty.
     * When order does not matter, then {@link StreamLine#findAny()} is recommended to keep the performance and close the stream as soon as possible.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @return An Optional describing the first element of this stream or an empty Optional if the stream is empty.
     */
    @Override
    public Optional<T> findFirst() {
        final T[] result = executeTerminal(true, false);
        return result.length == 0 ? Optional.empty() : Optional.ofNullable(result[0]);
    }

    /**
     * Returns an Optional describing some element of the stream, or an empty Optional if the stream is empty. This is a non-deterministic version of {@link StreamLine#findFirst()}.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @return An Optional describing some element of the stream, or an empty Optional if the stream is empty.
     */
    @Override
    public Optional<T> findAny() {
        final T[] array = executeTerminal(true, true);
        return Optional.ofNullable(array.length == 0 ? null : array[0]);
    }

    /**
     * Returns an iterator over the elements in this stream.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @return An Iterator over the elements in this stream.
     */
    @Override
    public Iterator<T> iterator() {
        return List.of(executeTerminal()).iterator();
    }

    /**
     * Creates a Spliterator over the elements in this stream.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @return (16464) A Spliterator over the elements in this stream.
     * This spliterator is consistent and independent of the result of the stream pipeline.
     */
    @Override
    public Spliterator<T> spliterator() {
        return List.of(executeTerminal()).spliterator();
    }

    /**
     * Returns a sequential stream considering all operations are to be performed in encounter order.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     * Sets {@link StreamLine#threads} to 1 - see {@link StreamLine#threads(int)}.
     *
     * @return A sequential Stream.
     */
    @Override
    public Stream<T> sequential() {
        threads(1);
        return this;
    }

    /**
     * Returns a possibly parallel stream considering all operations may be performed in any order.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     * Sets {@link StreamLine#threads} to 2 if it was 1 before - see {@link StreamLine#threads(int)}.
     *
     * @return A possibly parallel Stream.
     */
    @Override
    public Stream<T> parallel() {
        return threads == 1 ? this.threads(2) : this;
    }

    /**
     * Sets stream processing to unordered, which can improve performance. See also {@link StreamLine#ordered(boolean)}
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @return An unordered Stream.
     */
    @Override
    public Stream<T> unordered() {return this.ordered(false);}

    /**
     * Returns the same stream with a close handler attached.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param closeHandler A Runnable that will be executed when the stream is closed.
     * @return The same Stream with a close handler attached.
     */
    @Override
    public Stream<T> onClose(final Runnable closeHandler) {
        this.closeHandler = closeHandler;
        return this;
    }

    /**
     * Closes the stream, causing all close handlers for this stream pipeline to be executed.
     * {@link StreamLine} does have nothing to clean up, so this method is a no-op.
     */
    @Override
    public void close() {
        operations.clear();
        if (closeHandler != null) {
            closeHandler.run();
        }
    }

    /**
     * Returns whether this stream would execute tasks in parallel. <code>True</code> if the stream is parallel, otherwise false.
     *
     * @return True if <code>threads != 1</code>, otherwise false - see also {@link StreamLine#threads(int)} ().
     */
    @Override
    public boolean isParallel() {
        return threads != 1;
    }

    /**
     * Performs an action for each element of this stream <u><b>concurrently</b></u>, without regard to the order of elements.
     * This method does not guarantee thread safety; users must ensure that the provided action is thread-safe.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * <ul>
     * <li><b>forEach(Consumer):</b> Asynchronous, Concurrently, Unordered, No Thread Safety</li>
     * <li><b>{@link StreamLine#forEachSync(Consumer)}:</b> Synchronous, Unordered, Thread Safe</li>
     * <li><b>{@link StreamLine#forEachOrdered(Consumer)}:</b> Synchronous, Ordered, Thread Safe</li>
     * </ul>
     *
     * @param action A non-interfering action to perform on the elements of this stream.
     */
    @Override
    public void forEach(final Consumer<? super T> action) {
        forEachAsync(null, executeTerminal(false, false), pair -> action.accept(pair.getValue()));
    }

    /**
     * Performs an action for each element <u><b>synchronously</u></b>
     * It is required to take care of thread safety in the action.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * <ul>
     * <li><b>{forEachSync(Consumer):</b> Synchronous, Unordered, Thread Safe</li>
     * <li><b>{@link StreamLine#forEach(Consumer)}:</b> Asynchronous, Concurrently, Unordered, No Thread Safety</li>
     * <li><b>{@link StreamLine#forEachOrdered(Consumer)}:</b> Synchronous, Ordered, Thread Safe</li>
     * </ul>
     *
     * @param action A non-interfering action to perform on the elements of this stream.
     */
    public void forEachSync(final Consumer<? super T> action) {
        for (final T item : executeTerminal()) {
            action.accept(item);
        }
    }

    /**
     * Performs a synchronous action for each element of this stream, preserving the encounter order of the stream.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     * <ul>
     * <li><b>forEachOrdered(Consumer):</b> Synchronous, Ordered, Thread Safe</li>
     * <li><b>{@link StreamLine#forEach(Consumer)}:</b> Asynchronous, Concurrently, Unordered, No Thread Safety</li>
     * <li><b>{@link StreamLine#forEachSync(Consumer)}:</b> Synchronous, Unordered, Thread Safe</li>
     * </ul>
     *
     * @param action A non-interfering action to perform on the elements of this stream.
     */
    @Override
    public void forEachOrdered(final Consumer<? super T> action) {
        for (final T item : executeTerminal(true, false)) {
            action.accept(item);
        }
    }

    /**
     * Returns an array containing the elements of this stream.
     * The order depends on {@link StreamLine#ordered(boolean)}
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @return An array containing the elements of this stream.
     */
    @Override
    public Object[] toArray() {
        return executeTerminal();
    }

    /**
     * Returns an array containing the elements of this stream, using the provided generator function to allocate the returned array.
     * The order depends on {@link StreamLine#ordered(boolean)}
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param generator A function which produces a new array of the desired type and the provided length.
     * @return An array containing the elements of this stream.
     */
    @Override
    public <A> A[] toArray(final IntFunction<A[]> generator) {
        return List.of(executeTerminal()).toArray(generator);
    }

    /**
     * Performs a reduction on the elements of this stream, using the provided identity value and an associative accumulation function, and returns the reduced value.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param identity    The identity value for the accumulation function.
     * @param accumulator An associative, non-interfering, stateless function for combining two values.
     * @return The result of the reduction.
     */
    @Override
    public T reduce(final T identity, final BinaryOperator<T> accumulator) {
        T result = identity;
        for (final T item : executeTerminal()) {
            result = accumulator.apply(result, item);
        }
        return result;
    }

    /**
     * Performs a reduction on the elements of this stream using only a binary operator, starting from the first element as the initial value.
     * This variant of reduce does not take an identity value; thus, it returns an Optional to account for empty streams.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param accumulator An associative, non-interfering, stateless function for combining two values.
     * @return An Optional describing the result of the reduction or an empty Optional if the stream is empty.
     */
    @Override
    public Optional<T> reduce(final BinaryOperator<T> accumulator) {
        T result = null;
        for (final T item : executeTerminal()) {
            if (result == null) {
                result = item;  // The first item is the initial value
            } else {
                result = accumulator.apply(result, item);  // Apply the accumulator
            }
        }
        return Optional.ofNullable(result);
    }

    /**
     * Performs a reduction on the elements of this stream using an identity value, an accumulator, and a combiner function.
     * This method is intended for use where parallelism is involved, although this implementation does not specifically handle parallel execution.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param identity    The identity value for the accumulation function.
     * @param accumulator A function that takes two parameters: a partial result and the next element, and combines them.
     * @param combiner    A function used to combine the partial results. This is mainly used in a parallel context.
     * @return The result of the reduction.
     */
    @Override
    public <U> U reduce(final U identity, final BiFunction<U, ? super T, U> accumulator, final BinaryOperator<U> combiner) {
        U result = identity;
        for (final T item : executeTerminal()) {
            result = accumulator.apply(result, item);
        }
        return result;
    }

    @Override
    public <R> R collect(final Supplier<R> supplier, final BiConsumer<R, ? super T> accumulator, final BiConsumer<R, R> combiner) {
        final R[] results = Arrays.stream(executeTerminal()).map(item -> {
            final R container = supplier.get();
            accumulator.accept(container, item);
            return container;
        }).toArray(size -> (R[]) new Object[size]);
        final R resultContainer = supplier.get();
        for (final R result : results) {
            combiner.accept(resultContainer, result);
        }
        return resultContainer;
    }

    /**
     * Performs a mutable reduction operation on the elements of this stream using a collector.
     * A Collector encapsulates the functions used as arguments to collect(), which can accommodate a wide range of reduction operations.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param collector The collector encoding the reduction operation.
     * @return The result of the reduction.
     */

    @Override
    public <R, A> R collect(final Collector<? super T, A, R> collector) {
        final A resultContainer = collector.supplier().get();
        executeTerminal();
        for (final T item : executeTerminal()) {
            collector.accumulator().accept(resultContainer, item);
        }
        return collector.finisher().apply(resultContainer);
    }

    @Override
    public Optional<T> min(final Comparator<? super T> comparator) {
        return reduce(BinaryOperator.minBy(comparator));
    }

    @Override
    public Optional<T> max(final Comparator<? super T> comparator) {
        return reduce(BinaryOperator.maxBy(comparator));
    }

    public double sum() {
        return Arrays.stream(executeTerminal()).filter(Number.class::isInstance).map(Number.class::cast).mapToDouble(Number::doubleValue).sum();
    }

    public OptionalDouble max() {
        return Arrays.stream(executeTerminal()).filter(Number.class::isInstance).map(Number.class::cast).mapToDouble(Number::doubleValue).max();
    }

    public OptionalDouble min() {
        return Arrays.stream(executeTerminal()).filter(Number.class::isInstance).map(Number.class::cast).mapToDouble(Number::doubleValue).min();
    }

    public OptionalDouble average() {
        return Arrays.stream(executeTerminal()).filter(Number.class::isInstance).map(Number.class::cast).mapToDouble(Number::doubleValue).average();
    }

    public DoubleSummaryStatistics statistics() {
        return Arrays.stream(executeTerminal()).filter(Number.class::isInstance).map(Number.class::cast).mapToDouble(Number::doubleValue).summaryStatistics();
    }

    @Override
    public long count() {
        return executeTerminal().length;
    }

    /**
     * Determines whether any elements of this stream match the provided predicate. May not evaluate the predicate on all elements if not necessary for determining the result.
     * This is a short-circuiting terminal operation.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param predicate A predicate to apply to elements to determine a match.
     * @return true if any elements of the stream match the provided predicate, otherwise false.
     */
    @Override
    public boolean anyMatch(final Predicate<? super T> predicate) {
        final AtomicBoolean result = new AtomicBoolean(false);
        final AtomicBoolean terminate = new AtomicBoolean(false);
        forEachAsync(terminate, executeTerminal(), item -> {
            if (item.getValue() != null && predicate.test((T) item.getValue()) && !result.getAndSet(true)) {
                terminate.set(true);
            }
        });
        return result.get();
    }

    /**
     * Determines whether all elements of this stream match the provided predicate. May not evaluate the predicate on all elements if not necessary for determining the result.
     * This is a short-circuiting terminal operation.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param predicate A predicate to apply to elements to determine a match.
     * @return true if all elements of the stream match the provided predicate, otherwise false.
     */
    @Override
    public boolean allMatch(final Predicate<? super T> predicate) {
        final AtomicBoolean result = new AtomicBoolean(true);
        final AtomicBoolean terminate = new AtomicBoolean(false);
        forEachAsync(terminate, executeTerminal(), item -> {
            if (item.getValue() != null && !predicate.test((T) item.getValue()) && result.getAndSet(false)) {
                terminate.set(true);
            }
        });
        return result.get();
    }

    /**
     * Determines whether no elements of this stream match the provided predicate. May not evaluate the predicate on all elements if not necessary for determining the result.
     * This is a short-circuiting terminal operation.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param predicate A predicate to apply to elements to determine a match.
     * @return true if no elements of the stream match the provided predicate, otherwise false.
     */
    @Override
    public boolean noneMatch(final Predicate<? super T> predicate) {
        return !anyMatch(predicate);
    }

    protected <I> void forEachAsync(final AtomicBoolean terminate, final I[] values, final Consumer<Map.Entry<Integer, I>> runnable) {
        final Semaphore semaphore = threads > 0 ? new Semaphore(threads) : null;
        final List<Future<?>> futures = new ArrayList<>();
        final AtomicInteger index = new AtomicInteger(0);
        for (final I item : values) {
            final int currentIndex = index.getAndIncrement();
            try {
                if (terminate != null && terminate.get()) {
                    break;
                }
                if (semaphore != null) {
                    semaphore.acquire();
                }
                futures.add(executor.submit(() -> {
                    try {
                        runnable.accept(new AbstractMap.SimpleImmutableEntry<>(currentIndex, item));
                    } finally {
                        if (semaphore != null) {
                            semaphore.release();
                        }
                    }
                }));
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        waitFor(futures);
    }

    protected T[] executeTerminal() {
        return executeTerminal(ordered, false);
    }

    protected T[] executeTerminal(final boolean ordered, final boolean findAny) {
        final Map<Integer, T> indexedResults = new ConcurrentHashMap<>();
        final AtomicBoolean terminate = new AtomicBoolean(false);

        forEachAsync(terminate, source, item -> {
            if (findAny && !indexedResults.isEmpty()) {
                terminate.set(true);
            } else {
                final Object result = applyOperations(item.getValue());
                if (result != null) {
                    indexedResults.put(item.getKey(), (T) result);
                }
            }
        });

        // Collect results in the original order
        return ordered
            ? IntStream.range(0, source.length).mapToObj(indexedResults::get).filter(Objects::nonNull).toArray(size -> (T[]) new Object[size])
            : indexedResults.values().toArray((T[]) new Object[0]);

    }

    @SuppressWarnings("java:S4276") // Suppress SonarLint warning about unchecked cast
    protected Object applyOperations(final Object item) {
        Object result = item;
        for (final Function<Object, Object> operation : operations) {
            result = operation.apply(result);
            if (result == null) {
                break;
            }
        }
        return result;
    }

    public static void waitFor(final List<Future<?>> futures) {
        // Wait for all futures to complete
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (final ExecutionException ee) {
                final Throwable cause = ee.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new IllegalStateException("Exception in thread execution", cause);
                }
            }
        }
    }
}
