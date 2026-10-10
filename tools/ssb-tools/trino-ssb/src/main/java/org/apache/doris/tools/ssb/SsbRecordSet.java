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

package org.apache.doris.tools.ssb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class SsbRecordSet implements RecordSet {
    private final Path dbgen;
    private final SsbConnector.Table table;
    private final SsbConnector.Split split;
    private final List<Integer> columns;
    private final List<Type> types;

    SsbRecordSet(Path dbgen, SsbConnector.Table table, SsbConnector.Split split, List<Integer> columns) {
        this.dbgen = dbgen;
        this.table = table;
        this.split = split;
        this.columns = List.copyOf(columns);
        this.types = columns.stream()
                .map(index -> SsbTable.fromName(table.name()).columns.get(index).getType()).toList();
    }

    @Override
    public List<Type> getColumnTypes() {
        return types;
    }

    @Override
    public RecordCursor cursor() {
        return new Cursor();
    }

    private final class Cursor implements RecordCursor {
        private final Process process;
        private final BufferedReader reader;
        private final int fieldCount = SsbTable.fromName(table.name()).columns.size();
        private long completedBytes;
        private long readTimeNanos;
        private String[] fields;
        private boolean closed;

        Cursor() {
            ProcessBuilder builder = new ProcessBuilder(dbgen.toString(), "-q", "-s", table.schema().substring(2),
                    "-T", SsbTable.fromName(table.name()).dbgenOption,
                    "-b", dbgen.getParent().resolve("dists.dss").toString(),
                    "-C", Integer.toString(split.totalParts()), "-S", Integer.toString(split.part()));
            // mk_date uses localtime. All BEs must generate the same date dimension.
            builder.environment().put("TZ", "UTC");
            // Drain diagnostics without an extra thread or an unbounded stderr buffer.
            builder.redirectError(ProcessBuilder.Redirect.INHERIT);
            try {
                process = builder.start();
                reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.US_ASCII),
                        64 * 1024);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot start the operator-installed SSB generator: " + dbgen, e);
            }
        }

        @Override
        public boolean advanceNextPosition() {
            if (closed) {
                return false;
            }
            long start = System.nanoTime();
            try {
                String line = reader.readLine();
                if (line == null) {
                    int status = process.waitFor();
                    close();
                    if (status != 0) {
                        throw new IllegalStateException("SSB generator exited with " + status
                                + " for " + table + ", partition " + split);
                    }
                    return false;
                }
                fields = line.split("\\|", -1);
                if (fields.length != fieldCount + 1 || !fields[fieldCount].isEmpty()) {
                    throw new IllegalStateException("Invalid SSB row for " + table + ", partition " + split);
                }
                completedBytes += line.length() + 1;
                return true;
            } catch (IOException e) {
                close();
                throw new UncheckedIOException("Cannot read SSB data for " + table + ", partition " + split, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                close();
                throw new IllegalStateException("SSB generation interrupted", e);
            } catch (RuntimeException e) {
                close();
                throw e;
            } finally {
                readTimeNanos += System.nanoTime() - start;
            }
        }

        @Override
        public long getCompletedBytes() {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos() {
            return readTimeNanos;
        }

        @Override
        public Type getType(int field) {
            return types.get(field);
        }

        @Override
        public long getLong(int field) {
            return Long.parseLong(fields[columns.get(field)]);
        }

        @Override
        public Slice getSlice(int field) {
            return Slices.utf8Slice(fields[columns.get(field)]);
        }

        @Override
        public boolean isNull(int field) {
            columns.get(field);
            return false;
        }

        @Override
        public boolean getBoolean(int field) {
            throw new UnsupportedOperationException("SSB has no Boolean columns");
        }

        @Override
        public double getDouble(int field) {
            throw new UnsupportedOperationException("SSB has no floating point columns");
        }

        @Override
        public Object getObject(int field) {
            throw new UnsupportedOperationException("SSB has no object columns");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            // LIMIT, cancellation and scanner errors must also stop the producer. Killing before
            // closing the reader unblocks a producer waiting for space in the pipe.
            process.destroy();
            try {
                if (!process.waitFor(2, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor();
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            } finally {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException("Cannot close the SSB generator pipe", e);
                }
            }
        }
    }
}
