---
{
"title": "Java 微基准测试",
"language": "zh-CN"
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

# Java 微基准测试
Java 微基准测试（Java Microbenchmark Harness）是一个 Java 工具，用于构建、运行和分析用 Java 和其他针对 JVM 的语言编写的纳米/微米/毫/宏观基准测试。

## 为什么要使用 JMH
编写能够正确衡量大型应用程序的一小部分性能的基准是很困难的。当基准测试单独执行该组件时，JVM 或底层硬件可能会对您的组件应用许多优化。当组件作为大型
应用程序的一部分运行时，这些优化可能无法应用。因此，执行不当的微基准测试可能会让您相信您的组件的性能比实际情况要好。

编写正确的 Java 微基准通常需要防止 JVM 和硬件在微基准执行期间可能应用的优化，而这在实际生产系统中是不可能应用的。
这就是 为什么我们要使用 JMH。

## JMH 的特点
* **基准测试的正确性**。JMH 会尽可能地防止 JVM 和硬件优化基准测试代码，从而使基准测试结果更加可靠。
* **基准测试的可重复性**。JMH 会尽可能地防止外部因素（例如 GC）影响基准测试结果，从而使基准测试结果更加可靠。
* **基准测试的可比性**。JMH 会尽可能地防止外部因素（例如 CPU 频率）影响基准测试结果，从而使基准测试结果更加可靠。
* **基准测试的可扩展性**。JMH 可以在多个线程和多个进程中运行基准测试，从而使基准测试结果更加可靠。
* **基准测试的可配置性**。JMH 提供了大量的配置选项，以便用户可以根据需要进行调整。

## JMH 的使用

Apache Doris 提供了 `microbench` 模块，封装了一些编写 JMH 基准测试的基础类，因此，我们可以像编写普通的单元测试一样来编写基准测试代码，
但同时也可以自定义配置来覆盖默认配置。

我们鼓励在编写基准测试时，尽可能地使用 `microbench` 模块提供的基础类，这样可以减少编写基准测试的工作量，同时也可以保证基准测试的正确性。
同时对于一些性能敏感的代码，我们也鼓励编写基准测试来验证其性能。而所有基准测试的代码都应该放在 `microbench` 模块中。


example:

```java

import org.apache.doris.benchmark.BenchmarkBase;

public class BenchmarkExample extends AbstractMicrobenchmark {

    @Benchmark
    @Warmup(iterations = 3)// 预热次数
    public void testMethod() {
        // do something
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.MINUTES) // 测试模式，输出时间单位
    public void testMethod2() {
        // do something
    }
}

```
更多使用方法请参考 [JMH 官方文档](http://openjdk.java.net/projects/code-tools/jmh/)

