# StreamLine

StreamLine is a concurrent Stream API for workloads that benefit from virtual threads or a caller-provided
`ExecutorService`. It keeps the fluent Stream style, but gives you explicit control over worker count and chunk size so
you can tune how work is scheduled instead of hoping the default pool guesses correctly.

[![Build][build_shield]][build_link]
[![Maintainable][maintainable_shield]][maintainable_link]
[![Coverage][coverage_shield]][coverage_link]
[![Issues][issues_shield]][issues_link]
[![Commit][commit_shield]][commit_link]
[![Dependencies][dependency_shield]][dependency_link]
[![License][license_shield]][license_link]
[![Central][central_shield]][central_link]
[![Tag][tag_shield]][tag_link]
[![Javadoc][javadoc_shield]][javadoc_link]
[![Size][size_shield]][size_shield]
![Label][label_shield]
![Label][java_version]

## Motivation

Traditional Java streams are great for in-memory collection work, but `parallel()` is tied to the shared common pool
and offers little control when several concurrent workloads compete for the same runtime. StreamLine exists for the
cases where every element does real work such as I/O, waits, or heavier transformations and you want isolated,
explicit concurrency settings.

### Benefits

- **High-Performance Streaming**: Uses virtual threads by default and supports custom executors when isolation matters.
- **Simple API**: Offers a straightforward approach to parallel and asynchronous streaming operations.
- **Resource Management**: Lets callers own executor lifecycle instead of hiding it in a shared pool.
- **Enhanced Scalability**: Performs exceptionally well under high-load conditions, scaling effectively across multiple
  cores.
- **Pure Java**: No external dependencies for a lightweight integration.
- **Functional Design**: Embraces modern Java functional paradigms.
- **No Reflection**: Ensures compatibility with GraalVM native images.

### Prerequisites

* Java 21 or later and for using Project Loom

### Usage

Basic usage with the default virtual-thread executor:

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        StreamLine.of("one", "two", "three")
            .threads(-1) // unlimited workers
            .forEach(System.out::println);
    }
}
```

Bound concurrency with `threads(n)`:

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        final var result = StreamLine.range(0, 100)
            .threads(4) // at most 4 workers
            .map(value -> value * 2)
            .toList();

        System.out.println(result.size());
    }
}
```

Reduce scheduling overhead with `chunks(n)`:

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        final var result = StreamLine.range(0, 1_000)
            .threads(8)
            .chunks(32) // each worker drains up to 32 items before taking the next chunk
            .map(Example::loadRemoteValue)
            .toList();

        System.out.println(result.size());
    }

    private static int loadRemoteValue(final int value) {
        return value;
    }
}
```

Unlimited threads together with chunking means one worker per chunk:

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        StreamLine.range(0, 250)
            .threads(-1)
            .chunks(25) // 10 workers for 250 items
            .unordered()
            .forEach(System.out::println);
    }
}
```

Index-aware terminal operations:

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        StreamLine.of("gamma", "beta", "alpha")
            .sorted()
            .forEachOrdered((index, value) -> System.out.println(index + " -> " + value));
    }
}
```

Use a custom executor when you want separate scheduling or a hard cap:

```java
import berlin.yuna.streamline.model.StreamLine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Example {
    public static void main(final String[] args) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            final var result = StreamLine.of(executor, "one", "two", "three")
                .threads(4)
                .chunks(2)
                .map(String::toUpperCase)
                .toList();

            System.out.println(result);
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
```

### Scheduling Rules

- `threads(1)` runs sequentially.
- `threads(n)` with `n > 1` caps the number of active workers.
- `threads(-1)` uses unlimited workers and, when combined with `chunks(n)`, submits one worker per chunk.
- `chunks(1)` behaves like item-by-item scheduling.
- `chunks(n)` with `n > 1` lets each worker process a batch before claiming more work.
- `chunks(-1)` enables automatic chunk sizing and is the default.
- Negative values always fall back to the `-1` behavior instead of throwing.

### Ordered vs Unordered

- `ordered(true)` keeps encounter order in the result.
- `unordered()` skips result reordering and is usually the better default when downstream code does not care about
  stable ordering.

### When StreamLine Helps

| Workload                                                  | Java Stream \[A]             | Java Parallel Stream \[B]            | StreamLine \[C]     | Current Median Result                               |
|-----------------------------------------------------------|------------------------------|--------------------------------------|---------------------|-----------------------------------------------------|
| Small in-memory CPU-only mapping                          | Usually best                 | Often slightly worse than sequential | Usually unnecessary | **A `0.29 ms`**<br>B `2.21 ms`<br>C `9.25 ms`       |
| Blocking I/O per element                                  | Usually slow                 | Good while the common pool is free   | Strong fit          | A `496.68 ms`<br>B `52.16 ms`<br>**C `14.44 ms`**   |
| Many concurrent pipelines (`commonPoolParallelism() * 4`) | Often steadier than parallel | Can self-contend on the shared pool  | Strong fit          | A `124.55 ms`<br>B `456.91 ms`<br>**C `21.31 ms`**  |
| Custom executor isolation                                 | No                           | No                                   | Yes                 | Only StreamLine lets you isolate the work           |
| Tunable worker count and batching                         | No                           | No                                   | Yes                 | `threads(n)` and `chunks(n)` let you shape the load |

### Performance Notes

- `threads(n)` controls concurrency.
- `chunks(n)` controls batching.
- Small collections with very cheap lambdas are often faster with plain loops or standard sequential streams.
- StreamLine becomes more useful when every element does meaningful work and scheduling overhead is not the dominant
  cost.
- The main value proposition is not "faster than Java streams in every benchmark". The real win is avoiding the shared
  `ForkJoinPool` when several parallel stream workloads, frameworks, and libraries start competing for the same small
  server.
- Blocking or wait-heavy workloads and many concurrent pipelines are where StreamLine should be evaluated first.
- Benchmark through the real public entrypoint for your workload. Synthetic numbers without the real mapper or
  consumer are theater in a lab coat.

### Benchmark Command

Run the opt-in benchmark report:

```sh
mvn -q -Dtest=StreamLineBenchmarkTest -Dstreamline.benchmark=true test
```

The printed report includes:

- a cheap CPU-only single-stream case where plain Java usually wins
- a blocking single-stream case where StreamLine should shine
- a core-scaled concurrent common-pool case that reflects the "many pipelines on a small server" problem

### Limitations
* StreamLine is not compatible with Java 8
* StreamLine is mainly useful when each item does enough work to justify concurrent scheduling
* The concurrent processing does not extend to operations returning type-specific streams
  like `IntStream`, `LongStream`, `DoubleStream`, `OptionalInt`, `OptionalLong`, `OptionalDouble`, etc.
* StreamLine has more terminal operations than the usual java stream due its simple design - not sure if this is an advantage or disadvantage ^^

[build_shield]: https://github.com/YunaBraska/streamline/workflows/Daily/badge.svg

[build_link]: https://github.com/YunaBraska/streamline/actions?query=workflow%3Daily

[maintainable_shield]: https://img.shields.io/codeclimate/maintainability/YunaBraska/streamline?style=flat-square

[maintainable_link]: https://codeclimate.com/github/YunaBraska/streamline/maintainability

[coverage_shield]: https://img.shields.io/codeclimate/coverage/YunaBraska/streamline?style=flat-square

[coverage_link]: https://codeclimate.com/github/YunaBraska/streamline/test_coverage

[issues_shield]: https://img.shields.io/github/issues/YunaBraska/streamline?style=flat-square

[issues_link]: https://github.com/YunaBraska/streamline/commits/main

[commit_shield]: https://img.shields.io/github/last-commit/YunaBraska/streamline?style=flat-square

[commit_link]: https://github.com/YunaBraska/streamline/issues

[license_shield]: https://img.shields.io/github/license/YunaBraska/streamline?style=flat-square

[license_link]: https://github.com/YunaBraska/streamline/blob/main/LICENSE

[dependency_shield]: https://img.shields.io/librariesio/github/YunaBraska/streamline?style=flat-square

[dependency_link]: https://libraries.io/github/YunaBraska/streamline

[central_shield]: https://img.shields.io/maven-central/v/berlin.yuna/streamline?style=flat-square

[central_link]:https://search.maven.org/artifact/berlin.yuna/streamline

[tag_shield]: https://img.shields.io/github/v/tag/YunaBraska/streamline?style=flat-square

[tag_link]: https://github.com/YunaBraska/streamline/releases

[javadoc_shield]: https://javadoc.io/badge2/berlin.yuna/streamline/javadoc.svg?style=flat-square

[javadoc_link]: https://javadoc.io/doc/berlin.yuna/streamline

[size_shield]: https://img.shields.io/github/repo-size/YunaBraska/streamline?style=flat-square

[label_shield]: https://img.shields.io/badge/Yuna-QueenInside-blueviolet?style=flat-square

[gitter_shield]: https://img.shields.io/gitter/room/YunaBraska/streamline?style=flat-square

[gitter_link]: https://gitter.im/streamline/Lobby

[java_version]: https://img.shields.io/badge/java-21-blueviolet?style=flat-square
