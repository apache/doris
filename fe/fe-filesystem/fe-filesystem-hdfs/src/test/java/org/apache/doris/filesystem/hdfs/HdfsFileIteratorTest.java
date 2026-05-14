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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.spi.HadoopAuthenticator;
import org.apache.doris.filesystem.spi.IOCallable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;

/**
 * Unit tests for {@link HdfsFileIterator#close()} (Finding #9):
 * close must propagate to a {@link Closeable} {@link RemoteIterator} and be idempotent.
 */
class HdfsFileIteratorTest {

    private static final HadoopAuthenticator PASSTHROUGH = new HadoopAuthenticator() {
        @Override
        public <T> T doAs(IOCallable<T> action) throws IOException {
            return action.call();
        }
    };

    /** RemoteIterator that also implements Closeable so we can observe close propagation. */
    private static final class CloseableRemoteIterator
            implements RemoteIterator<FileStatus>, Closeable {
        int closeCount;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public FileStatus next() {
            throw new java.util.NoSuchElementException();
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    @Test
    void close_propagatesToCloseableDelegate() throws IOException {
        CloseableRemoteIterator delegate = new CloseableRemoteIterator();
        HdfsFileIterator it = new HdfsFileIterator(delegate, PASSTHROUGH);
        it.close();
        Assertions.assertEquals(1, delegate.closeCount,
                "close() must propagate to a Closeable RemoteIterator");
    }

    @Test
    void close_isIdempotent() throws IOException {
        CloseableRemoteIterator delegate = new CloseableRemoteIterator();
        HdfsFileIterator it = new HdfsFileIterator(delegate, PASSTHROUGH);
        it.close();
        it.close();
        it.close();
        Assertions.assertEquals(1, delegate.closeCount,
                "close() must be idempotent — delegate.close() called at most once");
    }

    @Test
    void close_noOpWhenDelegateNotCloseable() {
        // Plain RemoteIterator without Closeable — must not throw.
        RemoteIterator<FileStatus> plain = new RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public FileStatus next() {
                throw new java.util.NoSuchElementException();
            }
        };
        HdfsFileIterator it = new HdfsFileIterator(plain, PASSTHROUGH);
        Assertions.assertDoesNotThrow(it::close);
    }
}
