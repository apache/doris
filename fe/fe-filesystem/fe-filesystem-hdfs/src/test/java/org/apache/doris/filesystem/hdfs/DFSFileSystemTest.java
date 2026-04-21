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

import org.apache.doris.filesystem.Location;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link DFSFileSystem} constructor and lifecycle.
 * No real HDFS cluster required — tests focus on construction, close behavior,
 * and post-close error detection.
 */
class DFSFileSystemTest {

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    @Test
    void constructor_succeedsWithEmptyProperties() {
        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(new HashMap<>()),
                "DFSFileSystem should accept empty properties (simple auth, no Kerberos)");
    }

    @Test
    void constructor_succeedsWithHadoopUsername() {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.username", "testuser");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    @Test
    void constructor_succeedsWithHdfsProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns1");
        props.put("hadoop.username", "doris");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    // ------------------------------------------------------------------
    // close() and post-close behavior
    // ------------------------------------------------------------------

    @Test
    void close_isIdempotent() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();
        Assertions.assertDoesNotThrow(fs::close, "Second close should not throw");
    }

    @Test
    void exists_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("hdfs://namenode/test")));
        Assertions.assertTrue(ex.getMessage().contains("closed"),
                "Error message should indicate the filesystem is closed");
    }

    @Test
    void mkdirs_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.mkdirs(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void delete_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("hdfs://namenode/file"), false));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void rename_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("hdfs://nn/src"), Location.of("hdfs://nn/dst")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void list_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.list(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }
}
