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

package org.apache.doris.connector.iceberg;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Verifies the split-brain guard {@link IcebergAuthenticatedFileIO} adds to the iceberg write path: every FileIO
 * factory / delete call runs INSIDE the plugin authenticator's {@code doAs}, and each still reaches the delegate.
 *
 * <p>WHY it matters: {@code HadoopFileIO} captures the {@code FileSystem} (keyed by the current UGI) at
 * factory-call time. iceberg then dereferences the returned {@code OutputFile} on an unauthenticated worker-pool
 * thread, so ONLY a factory call made inside {@code doAs} captures the Kerberos FileSystem that the deferred
 * write reuses. A regression that forwards to the delegate WITHOUT {@code doAs} (delegate observes
 * {@code inDoAs == false}, or {@code doAsCount == 0}) hits secured HDFS as SIMPLE auth again — red here.
 */
public class IcebergAuthenticatedFileIOTest {

    @Test
    public void everyFactoryCallRunsInsideDoAsAndReachesDelegate() {
        RecordingAuthenticator auth = new RecordingAuthenticator();
        RecordingFileIO delegate = new RecordingFileIO(auth);
        IcebergAuthenticatedFileIO io = new IcebergAuthenticatedFileIO(delegate, auth);

        io.newOutputFile("hdfs://nn/out");
        io.newInputFile("hdfs://nn/in");
        io.newInputFile("hdfs://nn/in", 123L);
        io.deleteFile("hdfs://nn/del");

        Assertions.assertEquals(4, auth.doAsCount, "every factory/delete call must be wrapped in exactly one doAs");
        Assertions.assertEquals(
                Arrays.asList("newOutputFile", "newInputFile", "newInputFile", "deleteFile"),
                delegate.reachedInDoAs,
                "each op must reach the delegate AND run inside the authenticator's doAs");
        Assertions.assertFalse(auth.inDoAs, "doAs must have exited after each call");
    }

    @Test
    public void nonAuthMethodsForwardWithoutDoAs() {
        RecordingAuthenticator auth = new RecordingAuthenticator();
        RecordingFileIO delegate = new RecordingFileIO(auth);
        IcebergAuthenticatedFileIO io = new IcebergAuthenticatedFileIO(delegate, auth);

        io.properties();
        io.close();

        Assertions.assertEquals(0, auth.doAsCount, "pure delegation must not open a doAs");
        Assertions.assertTrue(delegate.closed, "close must reach the delegate");
    }

    /** Records doAs invocations and exposes whether an action is currently running inside a doAs. */
    private static final class RecordingAuthenticator implements HadoopAuthenticator {
        int doAsCount;
        boolean inDoAs;

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("wiring double: getUGI is unused (doAs is overridden)");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            doAsCount++;
            inDoAs = true;
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                inDoAs = false;
            }
        }
    }

    /** FileIO double: records the name of each factory call together with whether it ran inside a doAs. */
    private static final class RecordingFileIO implements FileIO {
        private final RecordingAuthenticator auth;
        final List<String> reachedInDoAs = new ArrayList<>();
        boolean closed;

        RecordingFileIO(RecordingAuthenticator auth) {
            this.auth = auth;
        }

        private void record(String op) {
            // Only record when observed inside the authenticator's doAs — the property under test.
            if (auth.inDoAs) {
                reachedInDoAs.add(op);
            } else {
                reachedInDoAs.add(op + ":NOT-in-doAs");
            }
        }

        @Override
        public InputFile newInputFile(String path) {
            record("newInputFile");
            return null;
        }

        @Override
        public OutputFile newOutputFile(String path) {
            record("newOutputFile");
            return null;
        }

        @Override
        public void deleteFile(String path) {
            record("deleteFile");
        }

        @Override
        public Map<String, String> properties() {
            return Collections.emptyMap();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
