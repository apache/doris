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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    private Env env = Mockito.mock(Env.class);

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @After
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(indexId, index.getId());
    }

    @Test
    public void testGetTabletsReturnsImmutableSnapshot() {
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        index.addTablet(new LocalTablet(1L), tabletMeta, true);

        List<Tablet> snapshot = index.getTablets();
        Assert.assertEquals(1, snapshot.size());

        // A write after the snapshot was taken must not be visible in it (copy-on-write).
        index.addTablet(new LocalTablet(2L), tabletMeta, true);
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(2, index.getTablets().size());

        // The returned snapshot is read-only.
        Assert.assertThrows(UnsupportedOperationException.class, () -> snapshot.add(new LocalTablet(3L)));
    }

    @Test
    public void testConcurrentGetTabletsNeverThrows() throws InterruptedException {
        // A reader repeatedly snapshots and iterates getTablets() while a writer keeps
        // adding tablets. Copy-on-write guarantees the reader never observes a partially
        // built list or throws ConcurrentModificationException.
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            long id = 1000L;
            while (!stop.get()) {
                index.addTablet(new LocalTablet(id++), tabletMeta, true);
                // Keep the list bounded (and exercise the clear path) so the test stays fast.
                if (index.getTablets().size() > 64) {
                    index.clearTabletsForRestore();
                }
            }
        });

        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 50000 && error.get() == null; i++) {
                    for (Tablet tablet : index.getTablets()) {
                        tablet.getId();
                    }
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                stop.set(true);
            }
        });

        writer.start();
        reader.start();
        reader.join();
        stop.set(true);
        writer.join();

        if (error.get() != null) {
            Assert.fail("getTablets() iteration threw under concurrent mutation: " + error.get());
        }
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./index"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        Text.writeString(dos, GsonUtils.GSON.toJson(index));

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        MaterializedIndex rIndex = GsonUtils.GSON.fromJson(Text.readString(dis), MaterializedIndex.class);
        Assert.assertEquals(index, rIndex);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
