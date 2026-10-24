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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.TvfGuard;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class AvroTvfAdmissionTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void inheritedLocalProviderIsRejectedForSchemaAndData() throws Exception {
        String xml = "<configuration><property><name>fs.hdfs.impl</name><value>"
                + LocalFileSystem.class.getName() + "</value></property></configuration>";
        Files.write(temporary.getRoot().toPath().resolve("core-site.xml"), xml.getBytes(StandardCharsets.UTF_8));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {temporary.getRoot().toURI().toURL()}, previous) {
            @Override
            public URL getResource(String name) {
                return "core-site.xml".equals(name) ? findResource(name) : super.getResource(name);
            }
        }) {
            Thread.currentThread().setContextClassLoader(loader);
            for (boolean schema : new boolean[] {true, false}) {
                HDFSFileReader reader = new HDFSFileReader("hdfs://runtime/does-not-exist.avro");
                reader.setTvfPolicy(TvfGuard.NATIVE_POLICY);
                IOException error = Assert.assertThrows(IOException.class,
                        () -> reader.open(new AvroFileContext(), schema));
                Assert.assertTrue(error.getMessage().contains("proven remote filesystem route"));
                Assert.assertNull(reader.fileSystem);
                Assert.assertNull(reader.schemaReader);
                Assert.assertNull(reader.dataReader);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            FileSystem.closeAll();
        }
    }

    @Test
    public void s3AvroCannotUseImplicitLocalProvider() throws Exception {
        String xml = "<configuration><property><name>fs.s3a.impl</name><value>"
                + LocalFileSystem.class.getName() + "</value></property></configuration>";
        Files.write(temporary.getRoot().toPath().resolve("core-site.xml"), xml.getBytes(StandardCharsets.UTF_8));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {temporary.getRoot().toURI().toURL()}, previous) {
            @Override
            public URL getResource(String name) {
                return "core-site.xml".equals(name) ? findResource(name) : super.getResource(name);
            }
        }) {
            Thread.currentThread().setContextClassLoader(loader);
            S3FileReader reader = new S3FileReader("access", "secret", "http://unused", "region", "s3://bucket/key.avro");
            reader.setTvfPolicy(TvfGuard.NATIVE_POLICY);
            Assert.assertThrows(IOException.class, () -> reader.open(new AvroFileContext(), true));
            Assert.assertNull(reader.fileSystem);
            Assert.assertNull(reader.schemaReader);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            FileSystem.closeAll();
        }
    }
}
