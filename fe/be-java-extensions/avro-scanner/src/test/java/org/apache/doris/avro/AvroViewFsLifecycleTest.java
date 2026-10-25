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

package org.apache.doris.avro;

import org.apache.doris.thrift.TFileType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.TvfGuard;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AvroViewFsLifecycleTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void guardedDataReusesOwnedFilesystemAndPreservesProjectionAndSplits() throws Exception {
        File file = records();
        try (RemoteClients clients = new RemoteClients(Files.readAllBytes(file.toPath()))) {
            int baseline = cacheSize();
            List<Integer> expected = projectedRows(new HDFSFileReader(file.toURI().toString()), file.length(), 0);
            Assert.assertEquals(20, expected.size());
            for (int index = 0; index < 8; index++) {
                Assert.assertEquals(expected, projectedRows(guardedReader(index), file.length(), 0));
                Assert.assertEquals(index + 1, clients.clients.size());
                clients.assertClosed();
                Assert.assertEquals(baseline, cacheSize());
            }
            long offset = file.length() / 2;
            List<Integer> expectedSplit = projectedRows(
                    new HDFSFileReader(file.toURI().toString()), file.length(), offset);
            Assert.assertFalse(expectedSplit.isEmpty());
            Assert.assertTrue(expectedSplit.size() < expected.size());
            Assert.assertEquals(expectedSplit, projectedRows(guardedReader(9), file.length(), offset));
            Assert.assertEquals(9, clients.clients.size());
            clients.assertClosed();
            Assert.assertEquals(baseline, cacheSize());
        }
    }

    @Test
    public void failedDataConstructionClosesInputAndOwnedClient() throws Exception {
        File file = records();
        try (RemoteClients clients = new RemoteClients(Files.readAllBytes(file.toPath()))) {
            int baseline = cacheSize();
            clients.invalidDataHeader = true;
            AvroJNIScanner scanner = scanner(clients, false, file.length());
            Assert.assertThrows(IOException.class, scanner::open);
            Field readerField = AvroJNIScanner.class.getDeclaredField("avroReader");
            readerField.setAccessible(true);
            Assert.assertNull(((AvroReader) readerField.get(scanner)).dataReader);
            Assert.assertEquals(2, clients.streams.size());
            Assert.assertEquals(1, clients.clients.size());
            clients.assertClosed();
            Assert.assertEquals(baseline, cacheSize());
        }
    }

    @Test
    public void closeFailureStillClosesDataReaderAndOwnedClient() throws Exception {
        File file = records();
        try (RemoteClients clients = new RemoteClients(Files.readAllBytes(file.toPath()))) {
            int baseline = cacheSize();
            HDFSFileReader reader = guardedReader(0);
            reader.open(context(file.length(), 0), false);
            Mockito.doThrow(new IOException("injected schema close failure")).when(clients.streams.get(0)).close();
            IOException failure = Assert.assertThrows(IOException.class, reader::close);
            Assert.assertEquals("injected schema close failure", failure.getMessage());
            Assert.assertEquals(1, clients.clients.size());
            clients.assertClosed();
            Assert.assertEquals(baseline, cacheSize());
        }
    }

    @Test
    public void failedProductionSchemaOpenClosesResourcesWithoutCallerClose() throws Exception {
        File file = records();
        for (boolean missing : new boolean[] {false, true}) {
            try (RemoteClients clients = new RemoteClients(Files.readAllBytes(file.toPath()))) {
                int baseline = cacheSize();
                clients.missingInput = missing;
                clients.truncatedSchema = !missing;
                AvroJNIScanner scanner = scanner(clients, true, file.length());
                Assert.assertThrows(IOException.class, scanner::open);
                Assert.assertEquals(1, clients.clients.size());
                Assert.assertEquals(missing ? 0 : 1, clients.streams.size());
                // JNI skips Java close after a failed open; no manual scanner.close here.
                clients.assertClosed();
                Assert.assertEquals(baseline, cacheSize());
            }
        }
    }

    @Test
    public void productionOpenPreservesFailureAndSuppressesCleanupErrors() throws Exception {
        File file = records();
        try (RemoteClients clients = new RemoteClients(Files.readAllBytes(file.toPath()))) {
            int baseline = cacheSize();
            clients.truncatedSchema = true;
            clients.failStreamClose = true;
            clients.failClientClose = true;
            AvroJNIScanner scanner = scanner(clients, true, file.length());
            IOException failure = Assert.assertThrows(IOException.class, scanner::open);
            Assert.assertFalse(failure.getMessage() != null && failure.getMessage().startsWith("injected"));
            Assert.assertEquals(2, failure.getSuppressed().length);
            clients.assertClosed();
            Assert.assertEquals(baseline, cacheSize());
        }
    }

    private AvroJNIScanner scanner(RemoteClients clients, boolean schemaOnly, long length) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(AvroProperties.IS_GET_TABLE_SCHEMA, Boolean.toString(schemaOnly));
        params.put(AvroProperties.FILE_TYPE, Integer.toString(TFileType.FILE_HDFS.getValue()));
        params.put(AvroProperties.URI, "viewfs://cluster/data/records.avro");
        params.put(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        params.put(AvroProperties.COLUMNS_TYPES, "int");
        params.put(AvroProperties.REQUIRED_FIELDS, "id");
        params.put(AvroProperties.HIVE_SERDE, "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        params.put(AvroProperties.SPLIT_START_OFFSET, "0");
        params.put(AvroProperties.SPLIT_SIZE, Long.toString(length));
        params.put(AvroProperties.SPLIT_FILE_SIZE, Long.toString(length));
        AvroJNIScanner scanner = new AvroJNIScanner(8, params);
        // Give the production scanner its isolated deployment resources, without mocking open/close.
        Field loaderField = AvroJNIScanner.class.getDeclaredField("classLoader");
        loaderField.setAccessible(true);
        loaderField.set(scanner, clients.loader);
        return scanner;
    }

    private HDFSFileReader guardedReader(int index) {
        HDFSFileReader reader = new HDFSFileReader("viewfs://cluster/data/records-" + index + ".avro");
        reader.setTvfPolicy(TvfGuard.NATIVE_POLICY);
        return reader;
    }

    private static AvroFileContext context(long length, long offset) {
        AvroFileContext context = new AvroFileContext();
        context.setRequiredFields(Collections.singleton("id"));
        context.setSplitStartOffset(offset);
        context.setSplitSize(length - offset);
        return context;
    }

    private static List<Integer> projectedRows(HDFSFileReader reader, long length, long offset) throws Exception {
        try {
            reader.open(context(length, offset), false);
            List<Integer> rows = new ArrayList<>();
            AvroWrapper<Pair<Integer, Long>> input = new AvroWrapper<>();
            while (reader.hasNext(input, NullWritable.get())) {
                GenericRecord record = (GenericRecord) reader.getNext();
                Assert.assertNull(record.getSchema().getField("name"));
                rows.add((Integer) record.get("id"));
            }
            return rows;
        } finally {
            reader.close();
        }
    }

    private File records() throws IOException {
        Schema schema = SchemaBuilder.record("row").fields()
                .requiredInt("id").requiredString("name").endRecord();
        File file = temporary.newFile("records.avro");
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, file);
            for (int id = 7; id < 27; id++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", id);
                record.put("name", "row-" + id);
                writer.append(record);
                writer.sync();
            }
        }
        return file;
    }

    private static int cacheSize() throws Exception {
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        Object cache = cacheField.get(null);
        Field mapField = cache.getClass().getDeclaredField("map");
        mapField.setAccessible(true);
        synchronized (cache) {
            return ((Map<?, ?>) mapField.get(cache)).size();
        }
    }

    /** Real guarded ViewFS/Avro dispatch; only the remote storage leaf is simulated. */
    private final class RemoteClients implements AutoCloseable {
        private final List<DistributedFileSystem> clients = new ArrayList<>();
        private final List<FSDataInputStream> streams = new ArrayList<>();
        private final MockedStatic<ReflectionUtils> factory;
        private final URLClassLoader loader;
        private final ClassLoader previousLoader;
        private final Method baseClose;
        private final byte[] bytes;
        private boolean invalidDataHeader;
        private boolean missingInput;
        private boolean truncatedSchema;
        private boolean failStreamClose;
        private boolean failClientClose;

        private RemoteClients(byte[] bytes) throws Exception {
            this.bytes = bytes;
            String xml = "<configuration>"
                    + "<property><name>fs.defaultFS</name><value>viewfs://cluster</value></property>"
                    + "<property><name>fs.viewfs.mounttable.cluster.link./data</name>"
                    + "<value>hdfs://recording/base</value></property>"
                    + "<property><name>fs.hdfs.impl.disable.cache</name><value>false</value></property>"
                    + "</configuration>";
            Files.write(temporary.getRoot().toPath().resolve("core-site.xml"), xml.getBytes(StandardCharsets.UTF_8));
            previousLoader = Thread.currentThread().getContextClassLoader();
            loader = new URLClassLoader(new URL[] {temporary.getRoot().toURI().toURL()}, previousLoader) {
                @Override
                public URL getResource(String name) {
                    return "core-site.xml".equals(name) ? findResource(name) : super.getResource(name);
                }
            };
            Thread.currentThread().setContextClassLoader(loader);
            baseClose = FileSystem.class.getDeclaredMethod("closeBase");
            baseClose.setAccessible(true);
            factory = Mockito.mockStatic(ReflectionUtils.class, Mockito.CALLS_REAL_METHODS);
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(DistributedFileSystem.class),
                    Mockito.any(Configuration.class))).thenAnswer(call -> remote(call.getArgument(1)));
        }

        private DistributedFileSystem remote(Configuration conf) throws Exception {
            DistributedFileSystem client = Mockito.mock(DistributedFileSystem.class);
            clients.add(client);
            AtomicReference<URI> uri = new AtomicReference<>();
            Mockito.when(client.getConf()).thenReturn(conf);
            Mockito.when(client.getUri()).thenAnswer(call -> uri.get());
            Mockito.when(client.getWorkingDirectory()).thenReturn(new Path("/"));
            Mockito.when(client.getHomeDirectory()).thenReturn(new Path("/"));
            Mockito.doAnswer(call -> {
                URI target = call.getArgument(0);
                uri.set(URI.create(target.getScheme() + "://" + target.getRawAuthority()));
                return null;
            }).when(client).initialize(Mockito.any(URI.class), Mockito.any(Configuration.class));
            Mockito.when(client.getFileStatus(Mockito.any(Path.class))).thenAnswer(call ->
                    new FileStatus(bytes.length, false, 1, 1, 0, call.getArgument(0)));
            Mockito.when(client.open(Mockito.any(Path.class), Mockito.anyInt())).thenAnswer(call -> {
                if (missingInput) {
                    throw new FileNotFoundException("recording Avro input is missing");
                }
                byte[] content = truncatedSchema ? new byte[] {'O', 'b', 'j', 1}
                        : invalidDataHeader && streams.size() % 2 == 1 ? new byte[] {0, 0, 0, 0} : bytes;
                SeekableByteArrayInput input = new SeekableByteArrayInput(content);
                FSDataInputStream stream = Mockito.mock(FSDataInputStream.class);
                streams.add(stream);
                if (failStreamClose) {
                    Mockito.doThrow(new IOException("injected stream close failure")).when(stream).close();
                }
                Mockito.when(stream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                        .thenAnswer(read -> input.read(read.getArgument(0), read.getArgument(1), read.getArgument(2)));
                Mockito.when(stream.read()).thenAnswer(read -> {
                    byte[] value = new byte[1];
                    return input.read(value, 0, 1) < 0 ? -1 : value[0] & 255;
                });
                Mockito.when(stream.getPos()).thenAnswer(read -> input.tell());
                Mockito.doAnswer(seek -> {
                    input.seek(seek.getArgument(0));
                    return null;
                }).when(stream).seek(Mockito.anyLong());
                return stream;
            });
            Mockito.doAnswer(call -> {
                baseClose.invoke(client);
                if (failClientClose) {
                    throw new IOException("injected client close failure");
                }
                return null;
            }).when(client).close();
            return client;
        }

        private void assertClosed() throws IOException {
            for (DistributedFileSystem client : clients) {
                Mockito.verify(client, Mockito.times(1)).close();
            }
            for (FSDataInputStream stream : streams) {
                Mockito.verify(stream, Mockito.atLeastOnce()).close();
            }
        }

        @Override
        public void close() throws Exception {
            try {
                FileSystem.closeAll();
            } finally {
                factory.close();
                Thread.currentThread().setContextClassLoader(previousLoader);
                loader.close();
            }
        }
    }
}
