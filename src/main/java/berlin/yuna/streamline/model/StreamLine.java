package berlin.yuna.streamline.model;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.*;

/**
 * {@link StreamLine} provides a concurrent Stream API backed by an exchangeable {@link ExecutorService}.
 * By default it uses {@link #VIRTUAL_EXECUTOR}, but callers can provide their own executor when they need separate
 * scheduling or stricter resource isolation.
 * <p><b>Scheduling controls:</b></p>
 * <ul>
 *     <li>{@link #threads(int)} controls how many workers may run concurrently.</li>
 *     <li>{@link #chunks(int)} controls how many items a worker drains before claiming more work.</li>
 *     <li>Negative values normalize to {@code -1}, which enables automatic scheduling instead of failing.</li>
 * </ul>
 * <p><b>Usage example:</b></p>
 * <pre>
 * {@link StreamLine}.range(0, 100)
 *     .threads(8)
 *     .chunks(16)
 *     .map(value -> value * 2)
 *     .toList();
 * </pre>
 * <p><b>Additional methods:</b></p>
 * <ul>
 *     <li>{@link #range(int, int)} mirrors {@link IntStream#range(int, int)}.</li>
 *     <li>{@link #count()}, {@link #sum()}, {@link #max()}, {@link #min()}, {@link #average()}, and
 *     {@link #statistics()} cover common numeric terminal operations.</li>
 *     <li>{@link #forEach(BiConsumer)}, {@link #forEachSync(BiConsumer)}, and
 *     {@link #forEachOrdered(BiConsumer)} expose the terminal index together with each value.</li>
 * </ul>
 * <p><b>Subclassing note:</b></p>
 * <p>Subclasses can override {@link #newStream(Object[])} to preserve their derived type across terminal pipeline
 * boundaries such as {@link #sorted()} or {@link #limit(long)}.</p>
 * <p><b>Concurrency note:</b></p>
 * <p>The indexed position provided by the indexed terminal operations is the position within the terminal result for
 * that invocation, not necessarily the original source position before filtering or sorting.</p>
 * <p><b>Limitations:</b></p>
 * <p>The concurrent processing does not extend to operations returning type-specific streams like {@link IntStream},
 * {@link LongStream}, {@link DoubleStream}, {@link OptionalInt}, {@link OptionalLong}, and
 * {@link OptionalDouble}.</p>
 *
 * @param <T> the type of the stream elements
 */
@SuppressWarnings({"unchecked", "java:S1905"}) // Suppress SonarLint warning about casting to T
public class StreamLine<T> implements Stream<T> {
    private static final int AUTO_CONFIG = -1;
    private static final int DEFAULT_CUSTOM_EXECUTOR_THREADS = 10;
    private static final int AUTO_CHUNKS_PER_BOUNDED_WORKER = 4;
    private static final int AUTO_CHUNKS_PER_UNLIMITED_WORKER = 8;
    private static final int MIN_INLINE_THRESHOLD = 64;
    private static final int MAX_INLINE_THRESHOLD = 512;
    private final T[] source;
    private final ExecutorService executor;
    private final List<Function<Object, Object>> operations = new ArrayList<>();
    private boolean ordered = true;
    private int threads;
    private int chunks = AUTO_CONFIG;
    private Runnable closeHandler;
    /**
     * Default virtual-thread executor used when callers do not provide a custom executor.
     */
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
        threads = executor == null ? Math.max(2, Runtime.getRuntime().availableProcessors() / 2) : DEFAULT_CUSTOM_EXECUTOR_THREADS;
    }

    /**
     * Creates a {@link StreamLine}. from given elements.
     *
     * @param <T> the type of the stream elements
     * @param values The elements to include in the new stream.
     * @return A new {@link StreamLine}.
     */
    public static <T> StreamLine<T> of(final T... values) {
        return of(null, values);
    }

    /**
     * Creates a {@link StreamLine}. from given elements and a custom executor.
     *
     * @param <T> the type of the stream elements
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
     * @param <T> the type of the stream elements
     * @param value The single element to create the stream from.
     * @return A new {@link StreamLine}..
     */
    public static <T> StreamLine<T> of(final T value) {
        return of(null, (T[]) new Object[]{value});
    }

    /**
     * Creates a {@link StreamLine}. from a single element with a custom executor.
     *
     * @param <T> the type of the stream elements
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
     * Gets the worker limit available for parallel processing.
     * {@code -1} enables automatic worker scheduling.
     *
     * @return the worker configuration
     */
    public int threads() {
        return threads;
    }

    /**
     * Sets the worker limit for parallel processing.
     * Negative values normalize to {@code -1}, which enables automatic worker scheduling.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param threads the number of workers to use
     * @return the current {@link StreamLine}
     */
    public StreamLine<T> threads(final int threads) {
        this.threads = threads < 0 ? AUTO_CONFIG : Math.max(1, threads);
        return this;
    }

    /**
     * Gets the chunk size configuration used when workers claim work.
     * {@code -1} enables automatic chunk sizing.
     *
     * @return the chunk size configuration
     */
    public int chunks() {
        return chunks;
    }

    /**
     * Sets the chunk size for parallel processing.
     * Negative values normalize to {@code -1}, which enables automatic chunk sizing.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param chunks the chunk size to use
     * @return the current {@link StreamLine}
     */
    public StreamLine<T> chunks(final int chunks) {
        this.chunks = chunks < 0 ? AUTO_CONFIG : Math.max(1, chunks);
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
    public <R> StreamLine<R> map(final Function<? super T, ? extends R> mapper) {
        operations.add((Function<Object, Object>) mapper);
        return (StreamLine<R>) this;
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
     * Differs from {@link Stream#map(Function)} which is an <a href="package-summary.html#StreamOps">intermediate operation</a>.
     * This closes the streams as soon as possible to keep things simple and continue with clean multi thread operations.
     *
     * @param mapper A function to apply to each element, which returns a stream of new values.
     * @return A new stream consisting of all elements produced by applying the function to each element.
     */
    @Override
    public <R> StreamLine<R> flatMap(final Function<? super T, ? extends Stream<? extends R>> mapper) {
        operations.add(item -> mapper.apply((T) item));
        return newStream(Arrays.stream(executeTerminal()).flatMap(item -> (Stream<R>) item).toArray(size -> (R[]) new Object[size]));
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
    public StreamLine<T> distinct() {
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
    public StreamLine<T> sorted() {
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
    public StreamLine<T> sorted(final Comparator<? super T> comparator) {
        final T[] values = executeTerminal();
        Arrays.sort(values, comparator);
        return newStream(values);
    }

    /**
     * Returns a stream consisting of the elements of this stream, each modified by the given function.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param action A non-interfering action to perform on the elements as they are consumed from the stream.
     * @return A stream consisting of the elements after applying the given action.
     */
    @Override
    public StreamLine<T> peek(final Consumer<? super T> action) {
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
    public StreamLine<T> limit(final long maxSize) {
        final T[] values = executeTerminal();
        return maxSize < 1 || maxSize > values.length ? this : newStream(Arrays.copyOfRange(values, 0, (int) Math.min(maxSize, values.length)));
    }

    /**
     * Returns a stream consisting of the remaining elements of this stream after discarding the first n elements.
     * <a href="package-summary.html#StreamOps">Terminal operation</a>
     *
     * @param n The number of leading elements to skip.
     * @return A new stream consisting of the remaining elements of this stream after skipping the first n elements.
     */
    @Override
    public StreamLine<T> skip(final long n) {
        final T[] values = executeTerminal();
        return n < 1 ? this : newStream(Arrays.copyOfRange(values, (int) Math.min(n, values.length), values.length));
    }

    /**
     * Returns a stream consisting of the elements of this stream that match the given predicate.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param predicate A predicate to apply to each element to determine if it should be included.
     * @return A stream consisting of the elements of this stream that match the given predicate.
     */
    @Override
    public StreamLine<T> filter(final Predicate<? super T> predicate) {
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
    public StreamLine<T> sequential() {
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
    public StreamLine<T> parallel() {
        return threads == 1 ? this.threads(2) : this;
    }

    /**
     * Sets stream processing to unordered, which can improve performance. See also {@link StreamLine#ordered(boolean)}
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @return An unordered Stream.
     */
    @Override
    public StreamLine<T> unordered() {return this.ordered(false);}

    /**
     * Returns the same stream with a close handler attached.
     * <a href="package-summary.html#StreamOps">Intermediate operation</a>
     *
     * @param closeHandler A Runnable that will be executed when the stream is closed.
     * @return The same Stream with a close handler attached.
     */
    @Override
    public StreamLine<T> onClose(final Runnable closeHandler) {
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
     * <li><b>{@link StreamLine#forEach(BiConsumer)}:</b> Asynchronous, Concurrently, Unordered, No Thread Safety, exposes terminal index</li>
     * <li><b>{@link StreamLine#forEachSync(Consumer)}:</b> Synchronous, Unordered, Thread Safe</li>
     * <li><b>{@link StreamLine#forEachOrdered(Consumer)}:</b> Synchronous, Ordered, Thread Safe</li>
     * </ul>
     *
     * @param action A non-interfering action to perform on the elements of this stream.
     */
    @Override
    public void forEach(final Consumer<? super T> action) {
        forEachAsync(null, executeTerminal(false, false), (index, value) -> action.accept(value));
    }

    /**
     * Performs an action for each terminal result <u><b>concurrently</b></u>, without regard to encounter order,
     * while also exposing the terminal index for that invocation.
     * This method does not guarantee thread safety; users must ensure that the provided action is thread-safe.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param action a non-interfering action receiving terminal index and value
     */
    public void forEach(final BiConsumer<Integer, ? super T> action) {
        forEachAsync(null, executeTerminal(false, false), action::accept);
    }

    /**
     * Performs an action for each element <u><b>synchronously</b></u>.
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
     * Performs an action for each element <u><b>synchronously</b></u> while also exposing the terminal index for that invocation.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param action a non-interfering action receiving terminal index and value
     */
    public void forEachSync(final BiConsumer<Integer, ? super T> action) {
        final T[] values = executeTerminal();
        for (int index = 0; index < values.length; index++) {
            action.accept(index, values[index]);
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
     * Performs a synchronous action for each element of this stream, preserving encounter order and exposing the terminal index.
     * <p><a href="package-summary.html#StreamOps">Terminal operation</a></p>.
     *
     * @param action a non-interfering action receiving terminal index and value
     */
    public void forEachOrdered(final BiConsumer<Integer, ? super T> action) {
        final T[] values = executeTerminal(true, false);
        for (int index = 0; index < values.length; index++) {
            action.accept(index, values[index]);
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
        return reduceSource(
            supplier,
            (container, item) -> accumulator.accept(container, (T) item),
            (left, right) -> {
                combiner.accept(left, right);
                return left;
            }
        );
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
        final A resultContainer = reduceSource(
            collector.supplier(),
            (container, item) -> collector.accumulator().accept(container, (T) item),
            collector.combiner()
        );
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

    /**
     * Sums all numeric terminal results and ignores non-numeric values.
     *
     * @return the numeric sum, or {@code 0} when no numeric values are present
     */
    public double sum() {
        return numericStatisticsOfSource().getSum();
    }

    /**
     * Finds the maximum numeric terminal result and ignores non-numeric values.
     *
     * @return the maximum numeric value when present
     */
    public OptionalDouble max() {
        final DoubleSummaryStatistics statistics = numericStatisticsOfSource();
        return statistics.getCount() < 1 ? OptionalDouble.empty() : OptionalDouble.of(statistics.getMax());
    }

    /**
     * Finds the minimum numeric terminal result and ignores non-numeric values.
     *
     * @return the minimum numeric value when present
     */
    public OptionalDouble min() {
        final DoubleSummaryStatistics statistics = numericStatisticsOfSource();
        return statistics.getCount() < 1 ? OptionalDouble.empty() : OptionalDouble.of(statistics.getMin());
    }

    /**
     * Calculates the average of all numeric terminal results and ignores non-numeric values.
     *
     * @return the numeric average when present
     */
    public OptionalDouble average() {
        final DoubleSummaryStatistics statistics = numericStatisticsOfSource();
        return statistics.getCount() < 1 ? OptionalDouble.empty() : OptionalDouble.of(statistics.getAverage());
    }

    /**
     * Calculates summary statistics for all numeric terminal results and ignores non-numeric values.
     *
     * @return numeric summary statistics for the terminal result
     */
    public DoubleSummaryStatistics statistics() {
        return numericStatisticsOfSource();
    }

    @Override
    public long count() {
        return reduceSource(
            () -> new long[1],
            (result, item) -> result[0]++,
            (left, right) -> {
                left[0] += right[0];
                return left;
            }
        )[0];
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
        forEachChunkAsync(terminate, source.length, (chunkIndex, startInclusive, endExclusive) -> {
            for (int index = startInclusive; index < endExclusive; index++) {
                if (shouldTerminate(terminate)) {
                    return;
                }
                final Object value = applyOperations(source[index]);
                if (value != null && predicate.test((T) value) && !result.getAndSet(true)) {
                    terminate.set(true);
                    return;
                }
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
        forEachChunkAsync(terminate, source.length, (chunkIndex, startInclusive, endExclusive) -> {
            for (int index = startInclusive; index < endExclusive; index++) {
                if (shouldTerminate(terminate)) {
                    return;
                }
                final Object value = applyOperations(source[index]);
                if (value != null && !predicate.test((T) value) && result.getAndSet(false)) {
                    terminate.set(true);
                    return;
                }
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

    private <I> void forEachAsync(final AtomicBoolean terminate, final I[] values, final IndexedConsumer<I> runnable) {
        forEachChunkAsync(terminate, values.length, (chunkIndex, startInclusive, endExclusive) ->
            runChunk(terminate, values, startInclusive, endExclusive, runnable)
        );
    }

    private void forEachChunkAsync(final AtomicBoolean terminate, final int valueCount, final ChunkConsumer consumer) {
        final int chunkSize = resolveChunkSize(valueCount);
        final int chunkCount = valueCount < 1 ? 0 : ceilDiv(valueCount, chunkSize);
        final int workerCount = resolveWorkerCount(valueCount);
        if (workerCount < 1) {
            return;
        }

        if (shouldInlineAutoWork(valueCount)) {
            consumer.accept(0, 0, valueCount);
            return;
        }

        if (workerCount == 1) {
            consumer.accept(0, 0, valueCount);
            return;
        }

        final int firstChunkEnd = shouldRunCallerChunk(valueCount, chunkSize) ? Math.min(chunkSize, valueCount) : 0;
        if (firstChunkEnd > 0) {
            consumer.accept(0, 0, firstChunkEnd);
            if (shouldTerminate(terminate) || firstChunkEnd >= valueCount) {
                return;
            }
        }

        if (threads < 0) {
            final List<Future<?>> futures = new ArrayList<>(chunkCount);
            for (int chunkIndex = firstChunkEnd / chunkSize; chunkIndex < chunkCount; chunkIndex++) {
                final int currentChunkIndex = chunkIndex;
                final int startInclusive = currentChunkIndex * chunkSize;
                futures.add(executor.submit(() -> consumer.accept(
                    currentChunkIndex,
                    startInclusive,
                    Math.min(startInclusive + chunkSize, valueCount)
                )));
            }
            waitFor(futures);
            return;
        }

        final AtomicInteger nextChunkIndex = new AtomicInteger(firstChunkEnd / chunkSize);
        final List<Future<?>> futures = new ArrayList<>(workerCount);
        for (int worker = 0; worker < workerCount; worker++) {
            futures.add(executor.submit(() -> {
                while (!shouldTerminate(terminate)) {
                    final int chunkIndex = nextChunkIndex.getAndIncrement();
                    final int startInclusive = chunkIndex * chunkSize;
                    if (startInclusive >= valueCount) {
                        return;
                    }
                    consumer.accept(chunkIndex, startInclusive, Math.min(startInclusive + chunkSize, valueCount));
                }
            }));
        }
        waitFor(futures);
    }

    private <R> R reduceSource(final Supplier<R> supplier, final BiConsumer<R, Object> accumulator, final BinaryOperator<R> combiner) {
        final int valueCount = source.length;
        if (valueCount < 1) {
            return supplier.get();
        }

        final int chunkCount = ceilDiv(valueCount, resolveChunkSize(valueCount));
        final Object[] partialResults = new Object[chunkCount];
        forEachChunkAsync(null, valueCount, (chunkIndex, startInclusive, endExclusive) -> {
            final R partialResult = supplier.get();
            for (int index = startInclusive; index < endExclusive; index++) {
                final Object result = applyOperations(source[index]);
                if (result != null) {
                    accumulator.accept(partialResult, result);
                }
            }
            partialResults[chunkIndex] = partialResult;
        });

        R result = supplier.get();
        for (final Object partialResult : partialResults) {
            if (partialResult != null) {
                result = combiner.apply(result, (R) partialResult);
            }
        }
        return result;
    }

    private DoubleSummaryStatistics numericStatisticsOfSource() {
        return reduceSource(
            DoubleSummaryStatistics::new,
            (statistics, item) -> {
                if (item instanceof Number number) {
                    statistics.accept(number.doubleValue());
                }
            },
            (left, right) -> {
                left.combine(right);
                return left;
            }
        );
    }

    /**
     * Executes the terminal pipeline using the configured ordering.
     *
     * @return the terminal results for the current pipeline
     */
    protected T[] executeTerminal() {
        return executeTerminal(ordered, false);
    }

    /**
     * Executes the terminal pipeline with explicit ordering and optional short-circuit behavior for {@code findAny()}.
     *
     * @param ordered whether terminal results should preserve encounter order
     * @param findAny whether execution may stop after the first successful result
     * @return the terminal results for the current pipeline
     */
    protected T[] executeTerminal(final boolean ordered, final boolean findAny) {
        if (findAny) {
            return executeFindAny();
        }

        final Object[] orderedResults = ordered ? new Object[source.length] : null;
        final Queue<T> unorderedResults = ordered ? null : new ConcurrentLinkedQueue<>();
        final AtomicBoolean terminate = new AtomicBoolean(false);

        forEachAsync(terminate, source, (index, value) -> {
            final Object result = applyOperations(value);
            if (result == null) {
                return;
            }

            if (ordered) {
                orderedResults[index] = result;
            } else {
                unorderedResults.add((T) result);
            }
        });

        return ordered
            ? Arrays.stream(orderedResults).map(orderedResult -> (T) orderedResult).filter(Objects::nonNull).toArray(size -> (T[]) new Object[size])
            : unorderedResults.toArray(size -> (T[]) new Object[size]);
    }

    /**
     * Applies all registered intermediate operations to a single value.
     *
     * @param item the source item to transform
     * @return the transformed result, or {@code null} when the item is filtered out
     */
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

    /**
     * Waits for all scheduled tasks to finish and rethrows task failures as runtime exceptions.
     *
     * @param futures the tasks to wait for
     */
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

    @SuppressWarnings("unchecked")
    private T[] executeFindAny() {
        final AtomicReference<T> firstResult = new AtomicReference<>();
        final AtomicBoolean terminate = new AtomicBoolean(false);

        forEachAsync(terminate, source, (index, value) -> {
            if (shouldTerminate(terminate)) {
                return;
            }
            final Object result = applyOperations(value);
            if (result != null && firstResult.compareAndSet(null, (T) result)) {
                terminate.set(true);
            }
        });

        final T result = firstResult.get();
        return result == null ? (T[]) new Object[0] : (T[]) new Object[]{result};
    }

    /**
     * Creates a derived stream instance used after terminal pipeline boundaries such as sorting, limiting, or flat-mapping.
     * Subclasses can override this to preserve their own type while reusing the current executor and scheduling setup.
     *
     * @param <R> the type of the derived stream elements
     * @param values the values for the derived stream
     * @return a derived {@link StreamLine} instance configured like the current stream
     */
    protected <R> StreamLine<R> newStream(final R[] values) {
        return StreamLine.of(executor, values).ordered(ordered).threads(threads).chunks(chunks);
    }

    private <I> void runChunk(final AtomicBoolean terminate, final I[] values, final int startInclusive, final int endExclusive, final IndexedConsumer<I> runnable) {
        for (int index = startInclusive; index < endExclusive; index++) {
            if (shouldTerminate(terminate)) {
                return;
            }
            runnable.accept(index, values[index]);
        }
    }

    private int resolveWorkerCount(final int valueCount) {
        if (valueCount < 1) {
            return 0;
        }
        if (threads == 1 || valueCount == 1) {
            return 1;
        }
        final int chunkCount = ceilDiv(valueCount, resolveChunkSize(valueCount));
        return threads > 0 ? Math.min(threads, chunkCount) : chunkCount;
    }

    private int resolveChunkSize(final int valueCount) {
        if (valueCount < 2) {
            return 1;
        }
        if (threads == 1) {
            return valueCount;
        }
        if (chunks > 0) {
            return Math.min(chunks, valueCount);
        }
        final int targetChunkCount = Math.min(valueCount, Math.max(1, detectedExecutorParallelism() * autoChunksPerWorker()));
        return Math.clamp(ceilDiv(valueCount, targetChunkCount), 1, valueCount);
    }

    private int detectedExecutorParallelism() {
        if (threads > 0) {
            return threads;
        }
        if (executor instanceof ForkJoinPool forkJoinPool) {
            return Math.max(1, forkJoinPool.getParallelism());
        }
        if (executor instanceof ThreadPoolExecutor threadPoolExecutor) {
            if (threadPoolExecutor.getMaximumPoolSize() > 0 && threadPoolExecutor.getMaximumPoolSize() < Integer.MAX_VALUE) {
                return threadPoolExecutor.getMaximumPoolSize();
            }
            if (threadPoolExecutor.getCorePoolSize() > 0) {
                return threadPoolExecutor.getCorePoolSize();
            }
        }
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    private int autoChunksPerWorker() {
        return threads > 0 ? AUTO_CHUNKS_PER_BOUNDED_WORKER : AUTO_CHUNKS_PER_UNLIMITED_WORKER;
    }

    private boolean shouldInlineAutoWork(final int valueCount) {
        return isAutomaticScheduling() && valueCount <= Math.clamp(detectedExecutorParallelism() * 16, MIN_INLINE_THRESHOLD, MAX_INLINE_THRESHOLD);
    }

    private boolean shouldRunCallerChunk(final int valueCount, final int chunkSize) {
        return isAutomaticScheduling() && valueCount > chunkSize;
    }

    private boolean isAutomaticScheduling() {
        return threads < 0 && chunks < 0;
    }

    private static int ceilDiv(final int dividend, final int divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    private static boolean shouldTerminate(final AtomicBoolean terminate) {
        return terminate != null && terminate.get();
    }

    @FunctionalInterface
    private interface IndexedConsumer<I> {
        void accept(int index, I value);
    }

    @FunctionalInterface
    private interface ChunkConsumer {
        void accept(int chunkIndex, int startInclusive, int endExclusive);
    }
}
