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
import org.apache.doris.filesystem.Location;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Unit tests for {@link ObjFileSystem}.
 *
 * <p>Covers:
 * <ul>
 *   <li>M4: {@link ObjFileSystem#isNotFoundError(IOException)} correctly classifies errors.</li>
 *   <li>M4: {@link ObjFileSystem#exists(Location)} returns false for not-found, re-throws others.</li>
 *   <li>M6: {@link ObjFileSystem#close()} delegates to the underlying {@link ObjStorage#close()}.</li>
 * </ul>
 *
 * <p>{@link ObjFileSystem} is abstract; a minimal {@link TestObjFileSystem} subclass is used
 * to test the concrete methods without requiring any cloud SDK dependencies.
 */
class ObjFileSystemTest {

    // ------------------------------------------------------------------
    // M4: isNotFoundError()
    // ------------------------------------------------------------------

    /**
     * M4: {@link FileNotFoundException} must be classified as a not-found error.
     */
    @Test
    void testIsNotFoundErrorForFileNotFoundException() {
        TestObjFileSystem fs = new TestObjFileSystem(new NoopObjStorage());
        Assertions.assertTrue(fs.isNotFoundError(new FileNotFoundException("key not found")),
                "FileNotFoundException must be a not-found error");
    }

    /**
     * M4: A plain {@link IOException} whose message contains "404" must NOT be classified as
     * not-found. Only {@link FileNotFoundException} qualifies — the fragile message-based
     * fallback was removed to avoid false positives (e.g., port numbers like "40404").
     */
    @Test
    void testIsNotFoundErrorFor404MessageIo() {
        TestObjFileSystem fs = new TestObjFileSystem(new NoopObjStorage());
        Assertions.assertFalse(fs.isNotFoundError(new IOException("HTTP 404 Not Found")),
                "Plain IOException with '404' in message must NOT be a not-found error");
    }

    /**
     * M4: A generic {@link IOException} (no "404" in message) must NOT be classified as not-found.
     */
    @Test
    void testIsNotFoundErrorForGenericIoException() {
        TestObjFileSystem fs = new TestObjFileSystem(new NoopObjStorage());
        Assertions.assertFalse(fs.isNotFoundError(new IOException("connection refused")),
                "Generic IOException must not be a not-found error");
    }

    // ------------------------------------------------------------------
    // M4: exists()
    // ------------------------------------------------------------------

    /**
     * M4: {@link ObjFileSystem#exists(Location)} must return {@code false} when
     * {@link ObjStorage#headObject(String)} throws {@link FileNotFoundException}.
     */
    @Test
    void testExistsReturnsFalseForFileNotFoundException() throws IOException {
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public RemoteObject headObject(String remotePath) throws IOException {
                throw new FileNotFoundException("no such key: " + remotePath);
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        Assertions.assertFalse(fs.exists(Location.of("s3://bucket/missing")),
                "exists() must return false when headObject() throws FileNotFoundException");
    }

    /**
     * M4: {@link ObjFileSystem#exists(Location)} must re-throw a plain {@link IOException}
     * even if the message happens to contain "404" — only {@link FileNotFoundException} qualifies.
     */
    @Test
    void testExistsRethrowsPlain404IOException() {
        IOException io404 = new IOException("404 Not Found");
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public RemoteObject headObject(String remotePath) throws IOException {
                throw io404;
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        IOException thrown = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("s3://bucket/missing")),
                "exists() must rethrow plain IOException even with '404' in message");
        Assertions.assertEquals(io404, thrown, "Rethrown exception must be the original");
    }

    /**
     * M4: {@link ObjFileSystem#exists(Location)} must return {@code true} when
     * {@link ObjStorage#headObject(String)} returns normally.
     */
    @Test
    void testExistsReturnsTrueWhenHeadObjectSucceeds() throws IOException {
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public RemoteObject headObject(String remotePath) throws IOException {
                return new RemoteObject(remotePath, remotePath, null, 42L, 0L);
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        Assertions.assertTrue(fs.exists(Location.of("s3://bucket/present")),
                "exists() must return true when headObject() succeeds");
    }

    /**
     * M4: {@link ObjFileSystem#exists(Location)} must re-throw non-not-found {@link IOException}.
     */
    @Test
    void testExistsRethrowsNonNotFoundIoException() {
        IOException networkError = new IOException("connection timed out");
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public RemoteObject headObject(String remotePath) throws IOException {
                throw networkError;
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        IOException thrown = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("s3://bucket/key")),
                "exists() must rethrow non-404 IOException");
        Assertions.assertEquals(networkError, thrown, "Rethrown exception must be the original");
    }

    // ------------------------------------------------------------------
    // M6: close()
    // ------------------------------------------------------------------

    /**
     * M6: {@link ObjFileSystem#close()} must delegate to {@link ObjStorage#close()}.
     */
    @Test
    void testCloseDelegatesToObjStorage() throws IOException {
        AtomicBoolean storageClosed = new AtomicBoolean(false);

        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public void close() throws IOException {
                storageClosed.set(true);
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        fs.close();

        Assertions.assertTrue(storageClosed.get(), "close() must call objStorage.close()");
    }

    /**
     * M6: An {@link IOException} thrown by {@link ObjStorage#close()} must propagate from
     * {@link ObjFileSystem#close()}.
     */
    @Test
    void testClosePropagatesObjStorageException() {
        IOException storageError = new IOException("storage close failed");
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public void close() throws IOException {
                throw storageError;
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        IOException thrown = Assertions.assertThrows(IOException.class, fs::close,
                "close() must propagate IOException from objStorage.close()");
        Assertions.assertEquals(storageError, thrown, "Propagated exception must be the original");
    }

    /**
     * M6: {@link ObjFileSystem#close()} is idempotent only if the underlying storage is.
     * This test verifies each call is forwarded (single delegation, not guarded).
     */
    @Test
    void testCloseCallCountForwardsToStorage() throws IOException {
        AtomicInteger callCount = new AtomicInteger(0);
        ObjStorage<?> storage = new NoopObjStorage() {
            @Override
            public void close() throws IOException {
                callCount.incrementAndGet();
            }
        };

        TestObjFileSystem fs = new TestObjFileSystem(storage);
        fs.close();
        Assertions.assertEquals(1, callCount.get(), "close() must call objStorage.close() exactly once");
    }

    // ------------------------------------------------------------------
    // Test infrastructure
    // ------------------------------------------------------------------

    /**
     * Minimal concrete subclass of {@link ObjFileSystem} for testing.
     * All abstract {@link org.apache.doris.filesystem.FileSystem} methods throw
     * {@link UnsupportedOperationException}; only the concrete ObjFileSystem methods
     * under test are exercised.
     */
    private static class TestObjFileSystem extends ObjFileSystem {

        TestObjFileSystem(ObjStorage<?> storage) {
            super("test", storage);
        }

        // Expose the protected method for white-box testing.
        @Override
        public boolean isNotFoundError(IOException e) {
            return super.isNotFoundError(e);
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
    }

    /**
     * No-op {@link ObjStorage} implementation for use in tests.
     * All methods throw {@link UnsupportedOperationException} except close() which is a no-op.
     * Tests override individual methods as needed.
     */
    private static class NoopObjStorage implements ObjStorage<Object> {

        @Override
        public Object getClient() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteObjects listObjects(String remotePath,
                String continuationToken) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteObject headObject(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putObject(String remotePath, RequestBody requestBody) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteObject(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyObject(String srcPath, String dstPath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String initiateMultipartUpload(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public UploadPartResult uploadPart(String remotePath, String uploadId,
                int partNum, RequestBody body) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void completeMultipartUpload(String remotePath, String uploadId,
                List<UploadPartResult> parts) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            // no-op by default
        }
    }
}
