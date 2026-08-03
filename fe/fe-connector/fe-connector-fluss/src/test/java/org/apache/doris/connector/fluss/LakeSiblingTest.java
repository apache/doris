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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

/**
 * The classloader pin every call into the lake sibling goes through.
 *
 * <p>Asserted against a classloader that is deliberately NOT the one the sibling was loaded by, because
 * in a unit test both this class and the stand-in sibling come from the same loader: without a third,
 * distinguishable one in play, "the pin happened" and "nothing happened" look identical.
 */
public class LakeSiblingTest {

    /** Stands in for the loader some other part of the engine had pinned before the call. */
    private ClassLoader callerLoader;
    private ClassLoader previous;

    @BeforeEach
    public void setUp() {
        previous = Thread.currentThread().getContextClassLoader();
        callerLoader = new URLClassLoader(new URL[0], previous);
        Thread.currentThread().setContextClassLoader(callerLoader);
    }

    @AfterEach
    public void tearDown() {
        Thread.currentThread().setContextClassLoader(previous);
    }

    /**
     * The sibling's SDK discovers its catalogs, file systems and file formats through the context
     * classloader. Left on the caller's, it looks for them in a plugin where none of them exist, and the
     * failure surfaces at the first lake read rather than at wiring time.
     */
    @Test
    public void theCallRunsUnderTheSiblingsOwnClassLoader() {
        RecordingLakeSibling sibling = new RecordingLakeSibling(Collections.emptyMap());

        ClassLoader seen = LakeSibling.call(sibling,
                () -> Thread.currentThread().getContextClassLoader());

        Assertions.assertSame(sibling.getClass().getClassLoader(), seen);
        Assertions.assertNotSame(callerLoader, seen);
    }

    /** What the caller had pinned is what the caller gets back; the pin is ours only for the call. */
    @Test
    public void theCallersClassLoaderIsRestoredAfterwards() {
        RecordingLakeSibling sibling = new RecordingLakeSibling(Collections.emptyMap());

        LakeSibling.call(sibling, () -> null);

        Assertions.assertSame(callerLoader, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Restored on the way out of a FAILING call too. A connector's errors are routine (a missing lake
     * table, an unreachable warehouse), so leaking the pin on that path would leave every later call in
     * the query resolving against the wrong plugin — with nothing to point at.
     */
    @Test
    public void theCallersClassLoaderIsRestoredWhenTheCallThrows() {
        RecordingLakeSibling sibling = new RecordingLakeSibling(Collections.emptyMap());

        Assertions.assertThrows(IllegalStateException.class,
                () -> LakeSibling.call(sibling, () -> {
                    throw new IllegalStateException("boom");
                }));

        Assertions.assertSame(callerLoader, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Building the sibling's metadata is itself a call into the sibling — it opens the lake catalog — so
     * it has to be inside the pin, not before it.
     */
    @Test
    public void buildingTheSiblingsMetadataIsPinnedToo() {
        LoaderProbe sibling = new LoaderProbe();

        LakeSibling.forward(new FlussTestSession(1L, "q1"), sibling,
                metadata -> metadata.getTableHandle(null, "db", "tbl"));

        Assertions.assertSame(sibling.getClass().getClassLoader(), sibling.loaderWhileBuilding);
    }

    /**
     * One metadata per statement, shared by every caller. Two instances would mean the scan planner and
     * the metadata gateway could see two different versions of the same lake table within one statement.
     */
    @Test
    public void theSiblingsMetadataIsBuiltOncePerStatement() {
        RecordingLakeSibling sibling = new RecordingLakeSibling(Collections.emptyMap());
        ConnectorSession session = new FlussTestSession(1L, "q1");

        LakeSibling.forward(session, sibling, metadata -> metadata.getTableHandle(null, "db", "tbl"));
        LakeSibling.forward(session, sibling, metadata -> metadata.getTableHandle(null, "db", "tbl"));

        Assertions.assertEquals(1, sibling.metadataBuilds);
    }

    /** A sibling that records the context classloader in force while its metadata is being built. */
    private static final class LoaderProbe implements Connector {

        private ClassLoader loaderWhileBuilding;

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            loaderWhileBuilding = Thread.currentThread().getContextClassLoader();
            return new ConnectorMetadata() {
            };
        }

        @Override
        public ConnectorScanPlanProvider getScanPlanProvider() {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public void close() {
        }
    }
}
