---
{
"title": "Java Microbenchmark Harness",
"language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Java Microbenchmark Harness
Java Microbenchmark Harness (JMH) is a Java harness for building, running, and analysing nano/micro/milli/macro 
benchmarks written in Java and other languages targeting the JVM.

## Why use JMH
Writing benchmarks that correctly measure the performance of a small part of a larger application is hard. There are 
many optimizations that the JVM or underlying hardware may apply to your component when the benchmark executes that 
component in isolation. These optimizations may not be possible to apply when the component is running as part of a 
larger application. Badly implemented microbenchmarks may thus make you believe that your component's performance is 
better than it will be in reality.↳

Writing a correct Java microbenchmark typically entails preventung the optimizations the JVM and hardware may apply 
during microbenchmark execution which could not have been applied in a real production system. 
That is what JMH - the Java Microbenchmark Harness - is helping you do.

## Features of JMH
* **Benchmarked correctness**. JMH prevents the JVM and hardware from optimizing the benchmark code as much as possible, making the benchmark results more reliable.
* **Repeatability of benchmarks**. JMH tries to prevent external factors (such as GC) from affecting the benchmark results as much as possible, so that the benchmark results are more reliable.
* **Comparability of benchmarks**. JMH makes benchmark results more reliable by preventing external factors such as CPU frequency from affecting benchmark results as much as possible.
* **Scalability of benchmarks**. JMH can run benchmarks in multiple threads and processes, making benchmark results more reliable.
* **Benchmark Configurability**. JMH provides a large number of configuration options so that users can adjust it according to their needs.

## How to use JMH

Apache Doris provides the `microbench` module, which encapsulates some basic classes for writing JMH benchmarks, 
so we can write benchmark codes like ordinary unit tests,
But it is also possible to customize the configuration to override the default configuration.

We encourage you to use the basic classes provided by the `microbench` module as much as possible when writing 
benchmarks, which can reduce the workload of writing benchmarks and ensure the correctness of benchmarks.
At the same time, for some performance-sensitive code, we also encourage writing benchmark tests to verify its 
performance. All benchmark code should be placed in the `microbench` module

example:

```java

import org.apache.doris.benchmark.BenchmarkBase;

public class BenchmarkExample extends AbstractMicrobenchmark {

    @Benchmark
    @Warmup(iterations = 3)// warmup 3 times
    public void testMethod() {
        // do something
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES) // throughput mode, output time unit is minutes
    public void testMethod2() {
        // do something
    }
}

```
more information about JMH, please refer to [JMH](http://openjdk.java.net/projects/code-tools/jmh/)

