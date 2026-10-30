// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.deltalake;

import org.apache.doris.thrift.TDeltaLakeFileDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.metrics.ScanReport;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Batch-boundary timing probe, separate from the actual-source scale acceptance. */
@EnabledIfSystemProperty(named = "deltalake.planning.table", matches = ".+")
class DeltaLakePlanningMetricsTest {
    @Test
    void measuresReadDecodeReplayPruningAndRangeConstruction() throws Exception {
        TimedEngine engine = new TimedEngine(DefaultEngine.create(new Configuration()));
        Snapshot snapshot = TableManager.loadSnapshot(System.getProperty("deltalake.planning.table")).build(engine);
        for (boolean filtered : new boolean[] {false, true}) {
            engine.readNanos.set(0);
            engine.parseNanos.set(0);
            engine.expressionNanos.set(0);
            Optional<Predicate> predicate = filtered
                    ? Optional.of(new Predicate("<", new Column("id"), Literal.ofLong(0))) : Optional.empty();
            long kernelNanos = 0;
            long conversionNanos = 0;
            long selected = 0;
            long bytes = 0;
            try (CloseableIterator<Row> files = DeltaLakeScanFiles.open(snapshot, engine, false, predicate)) {
                while (true) {
                    long started = System.nanoTime();
                    boolean more = files.hasNext();
                    Row row = more ? files.next() : null;
                    kernelNanos += System.nanoTime() - started;
                    if (!more) {
                        break;
                    }
                    started = System.nanoTime();
                    FileStatus file = DeltaKernelCompat.fileStatus(row);
                    DeltaLakeScanRange range = new DeltaLakeScanRange(file.getPath(), 0, file.getSize(), file.getSize(),
                            file.getModificationTime(), 128L << 20, Map.of(), new TDeltaLakeFileDesc());
                    bytes += range.getLength();
                    selected++;
                    conversionNanos += System.nanoTime() - started;
                }
            }
            Assertions.assertEquals(filtered ? 0 : 1_000_000, selected);
            Assertions.assertEquals(filtered ? 0 : 527_000_000, bytes);
            Assertions.assertTrue(engine.report.getIsFullyConsumed());
            Assertions.assertEquals(1_000_000, engine.report.getScanMetrics().getNumActiveAddFiles());
            long read = engine.readNanos.get();
            long parse = engine.parseNanos.get();
            long expression = engine.expressionNanos.get();
            System.out.printf("DELTA_PHASES filtered=%s read_decode_ms=%.3f stats_parse_ms=%.3f "
                            + "expression_ms=%.3f replay_iteration_ms=%.3f range_dto_ms=%.3f selected=%d%n",
                    filtered, read / 1e6, parse / 1e6, expression / 1e6,
                    (kernelNanos - read - parse - expression) / 1e6, conversionNanos / 1e6, selected);
        }
    }

    private static final class TimedEngine implements Engine {
        private final Engine delegate;
        private final AtomicLong readNanos = new AtomicLong();
        private final AtomicLong parseNanos = new AtomicLong();
        private final AtomicLong expressionNanos = new AtomicLong();
        private ScanReport report;

        private TimedEngine(Engine delegate) {
            this.delegate = delegate;
        }

        @Override
        public ExpressionHandler getExpressionHandler() {
            return (ExpressionHandler) Proxy.newProxyInstance(ExpressionHandler.class.getClassLoader(),
                    new Class<?>[] {ExpressionHandler.class}, (proxy, method, args) -> {
                        Object result = invoke(delegate.getExpressionHandler(), method, args);
                        if (result instanceof PredicateEvaluator) {
                            return timed(PredicateEvaluator.class, (PredicateEvaluator) result, expressionNanos);
                        }
                        if (result instanceof ExpressionEvaluator) {
                            return timed(ExpressionEvaluator.class, (ExpressionEvaluator) result, expressionNanos);
                        }
                        return result;
                    });
        }

        @Override
        public JsonHandler getJsonHandler() {
            return (JsonHandler) Proxy.newProxyInstance(JsonHandler.class.getClassLoader(),
                    new Class<?>[] {JsonHandler.class}, (proxy, method, args) ->
                            measure(delegate.getJsonHandler(), method, args,
                                    "parseJson".equals(method.getName()) ? parseNanos : readNanos));
        }

        @Override
        public ParquetHandler getParquetHandler() {
            return timed(ParquetHandler.class, delegate.getParquetHandler(), readNanos);
        }

        @Override
        public FileSystemClient getFileSystemClient() {
            return delegate.getFileSystemClient();
        }

        @Override
        public List<MetricsReporter> getMetricsReporters() {
            return List.of(value -> {
                if (value instanceof ScanReport) {
                    report = (ScanReport) value;
                }
            });
        }

        private static <T> T timed(Class<T> type, T delegate, AtomicLong elapsed) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                    (proxy, method, args) -> measure(delegate, method, args, elapsed)));
        }

        private static Object measure(Object delegate, Method method, Object[] args, AtomicLong elapsed)
                throws Throwable {
            long started = System.nanoTime();
            Object result;
            try {
                result = invoke(delegate, method, args);
            } finally {
                elapsed.addAndGet(System.nanoTime() - started);
            }
            if (result instanceof CloseableIterator) {
                CloseableIterator<?> iterator = (CloseableIterator<?>) result;
                return new CloseableIterator<Object>() {
                    @Override
                    public boolean hasNext() {
                        long started = System.nanoTime();
                        try {
                            return iterator.hasNext();
                        } finally {
                            elapsed.addAndGet(System.nanoTime() - started);
                        }
                    }

                    @Override
                    public Object next() {
                        long started = System.nanoTime();
                        try {
                            return iterator.next();
                        } finally {
                            elapsed.addAndGet(System.nanoTime() - started);
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        long started = System.nanoTime();
                        try {
                            iterator.close();
                        } finally {
                            elapsed.addAndGet(System.nanoTime() - started);
                        }
                    }
                };
            }
            return result;
        }

        private static Object invoke(Object delegate, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
