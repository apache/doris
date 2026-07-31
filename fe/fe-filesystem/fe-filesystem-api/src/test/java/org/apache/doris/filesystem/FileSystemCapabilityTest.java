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

package org.apache.doris.filesystem;

import org.apache.doris.filesystem.capability.BatchDeleteCapability;
import org.apache.doris.filesystem.capability.Capability;
import org.apache.doris.filesystem.capability.MultipartUploadCapability;
import org.apache.doris.filesystem.capability.PresignedUrlCapability;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

class FileSystemCapabilityTest {

    @Test
    void capabilityInterfacesAreMarkedAsCapability() {
        Assertions.assertTrue(Capability.class.isAssignableFrom(PresignedUrlCapability.class));
        Assertions.assertTrue(Capability.class.isAssignableFrom(BatchDeleteCapability.class));
        Assertions.assertTrue(Capability.class.isAssignableFrom(MultipartUploadCapability.class));
    }

    @Test
    void capability_returnsEmptyByDefault() {
        FileSystem fs = new NoCapabilityFileSystem();

        Optional<PresignedUrlCapability> capability = fs.capability(PresignedUrlCapability.class);

        Assertions.assertTrue(capability.isEmpty());
    }

    @Test
    void requireCapability_throwsWhenCapabilityIsMissing() {
        FileSystem fs = new NoCapabilityFileSystem();

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> fs.requireCapability(PresignedUrlCapability.class));

        Assertions.assertTrue(exception.getMessage().contains("PresignedUrlCapability"));
    }

    @Test
    void requireCapability_returnsProviderCapability() {
        PresignedUrlCapability presignedUrl = (location, expiration) ->
                "https://example.test/" + location.uri() + "?ttl=" + expiration.getSeconds();
        FileSystem fs = new CapabilityFileSystem(presignedUrl);

        PresignedUrlCapability capability = fs.requireCapability(PresignedUrlCapability.class);

        Assertions.assertSame(presignedUrl, capability);
    }

    private static class NoCapabilityFileSystem implements FileSystem {
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

    private static class CapabilityFileSystem extends NoCapabilityFileSystem {
        private final PresignedUrlCapability presignedUrl;

        private CapabilityFileSystem(PresignedUrlCapability presignedUrl) {
            this.presignedUrl = presignedUrl;
        }

        @Override
        public <T extends Capability> Optional<T> capability(Class<T> capabilityType) {
            if (capabilityType == PresignedUrlCapability.class) {
                return Optional.of(capabilityType.cast(presignedUrl));
            }
            return Optional.empty();
        }
    }
}
