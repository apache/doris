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

package org.apache.doris.filesystem.spi;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

class FileSystemProviderTypedPropertiesTest {

    @Test
    void provider_canBindAndCreateWithTypedProperties() throws IOException {
        TypedProvider provider = new TypedProvider();
        TestProperties properties = provider.bind(Map.of("test.key", "value"));

        FileSystem fileSystem = provider.create(properties);

        Assertions.assertEquals("value", properties.value);
        Assertions.assertSame(properties, ((TestFileSystem) fileSystem).properties);
    }

    private static class TypedProvider implements FileSystemProvider<TestProperties> {
        @Override
        public boolean supports(Map<String, String> properties) {
            return properties.containsKey("test.key");
        }

        @Override
        public TestProperties bind(Map<String, String> properties) {
            return new TestProperties(properties.get("test.key"));
        }

        @Override
        public FileSystem create(TestProperties properties) {
            return new TestFileSystem(properties);
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            return create(bind(properties));
        }
    }

    private static class TestProperties implements FileSystemProperties {
        private final String value;

        private TestProperties(String value) {
            this.value = value;
        }

        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.LOCAL;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.FILE;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> toFileSystemKv() {
            return Map.of("test.key", value);
        }
    }

    private static class TestFileSystem implements FileSystem {
        private final TestProperties properties;

        private TestFileSystem(TestProperties properties) {
            this.properties = properties;
        }

        @Override
        public boolean exists(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mkdirs(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Location location, boolean recursive) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(Location src, Location dst) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator list(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisInputFile newInputFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisOutputFile newOutputFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
        }
    }
}
