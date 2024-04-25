# StreamLine

StreamLine is an enhanced Java Stream API optimized for concurrent processing, leveraging the power of Project Loom's
virtual threads. Designed to provide superior performance in multithreaded environments, it simplifies the usage of
streams without the common pitfalls of resource management in standard Java streams.

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

Traditional Java streams are powerful but also come with big limits cause of the shared ForkedJoinPool which is not
replaceable and also not programmatically configurable.
Java's parallel streams start blocking each other in concurrent environments, leading to performance bottlenecks.
Therefore, StreamLine was created to address these shortcomings.

### Benefits

- **High-Performance Streaming**: Takes full advantage of Project Loom's virtual threads for efficient non-blocking
  concurrency.
- **Simple API**: Offers a straightforward approach to parallel and asynchronous streaming operations.
- **Resource Management**: Designed to avoid typical issues related to stream resource management, ensuring cleaner and
  safer code.
- **Enhanced Scalability**: Performs exceptionally well under high-load conditions, scaling effectively across multiple
  cores.
- **Pure Java**: No external dependencies for a lightweight integration.
- **Functional Design**: Embraces modern Java functional paradigms.
- **No Reflection**: Ensures compatibility with GraalVM native images.

### Prerequisites

* Java 21 or later and for using Project Loom

### Usage

```java
import berlin.yuna.streamline.model.StreamLine;

public class Example {
    public static void main(final String[] args) {
        StreamLine.of("one", "two", "three")
            .threads(-1) // Use unlimited threads
            .forEach(System.out::println);
    }
}
```

### StreamLine Performance

Each method is tested with 10 concurrent streams including 10 tasks for every stream.
CPU cores: 10.

| Method                    | Time  |
|---------------------------|-------|
| Loop \[for]               | 1.86s |
| Java Stream \[Sequential] | 1.86s |
| Java Stream \[Parallel]   | 724ms |
| StreamLine \[Ordered]     | 118ms |
| StreamLine \[Unordered]   | 109ms |
| StreamLine \[2 Threads]   | 512ms |

### Limitations

* The concurrent processing does not extend to operations returning type-specific streams
  like `IntStream`, `LongStream`, `DoubleStream`, `OptionalInt`, `OptionalLong`, `OptionalDouble`, etc.
* StreamLine has more Terminal operations than the usual java stream due its simple design

[build_shield]: https://github.com/YunaBraska/streamline/workflows/MVN_RELEASE/badge.svg

[build_link]: https://github.com/YunaBraska/streamline/actions?query=workflow%3AMVN_RELEASE

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
