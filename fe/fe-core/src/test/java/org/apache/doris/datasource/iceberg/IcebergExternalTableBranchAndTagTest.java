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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.BranchOptions;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TagOptions;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class IcebergExternalTableBranchAndTagTest {

    Path tempDirectory;
    Table icebergTable;
    IcebergExternalCatalog catalog;
    IcebergExternalDatabase db;
    IcebergExternalTable dorisTable;
    HadoopCatalog icebergCatalog;
    MockedStatic<IcebergUtils> mockedIcebergUtils;
    MockedStatic<Env> mockedEnv;
    String dbName = "db";
    String tblName = "tbl";

    @BeforeEach
    public void setUp() throws IOException {
        HashMap<String, String> map = new HashMap<>();
        tempDirectory = Files.createTempDirectory("");
        map.put("warehouse", "file://" + tempDirectory.toString());
        map.put("type", "hadoop");
        System.out.println(tempDirectory);
        icebergCatalog =
                (HadoopCatalog) CatalogUtil.buildIcebergCatalog("iceberg_catalog", map, new Configuration());

        // init iceberg table
        icebergCatalog.createNamespace(Namespace.of(dbName));
        icebergTable = icebergCatalog.createTable(
                TableIdentifier.of(dbName, tblName),
            new Schema(Types.NestedField.required(1, "level", Types.StringType.get())));

        // init external table
        catalog = Mockito.spy(new IcebergHadoopExternalCatalog(1L, "iceberg", null, map, null));
        catalog.setInitializedForTest(true);
        // db = new IcebergExternalDatabase(catalog, 1L, dbName, dbName);
        db = Mockito.spy(new IcebergExternalDatabase(catalog, 1L, dbName, dbName));
        dorisTable = Mockito.spy(new IcebergExternalTable(1, tblName, tblName, catalog, db));
        Mockito.doReturn(db).when(catalog).getDbNullable(Mockito.any());
        Mockito.doReturn(dorisTable).when(db).getTableNullable(Mockito.any());

        // mock IcebergUtils.getIcebergTable to return our test icebergTable
        mockedIcebergUtils = Mockito.mockStatic(IcebergUtils.class);
        mockedIcebergUtils.when(() -> IcebergUtils.getIcebergTable(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(icebergTable);

        // mock Env.getCurrentEnv().getEditLog().logBranchOrTag(info) to do nothing
        Env mockEnv = Mockito.mock(Env.class);
        EditLog mockEditLog = Mockito.mock(EditLog.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getEditLog()).thenReturn(mockEditLog);
        Mockito.doNothing().when(mockEditLog).logBranchOrTag(Mockito.any());
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (icebergCatalog != null) {
            icebergCatalog.dropTable(TableIdentifier.of("db", "tbl"));
            icebergCatalog.dropNamespace(Namespace.of("db"));
        }
        Files.deleteIfExists(tempDirectory);

        // close the static mock
        if (mockedIcebergUtils != null) {
            mockedIcebergUtils.close();
        }
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    @Test
    public void testCreateTagWithTable() throws UserException, IOException {
        String tag1 = "tag1";
        String tag2 = "tag2";
        String tag3 = "tag3";

        // create a new tag: tag1
        // will fail
        CreateOrReplaceTagInfo info =
                new CreateOrReplaceTagInfo(tag1, true, false, false, TagOptions.EMPTY);
        Assertions.assertThrows(
                UserException.class,
                () -> catalog.createOrReplaceTag(dbName, tblName, info));

        // add some data
        addSomeDataIntoIcebergTable();
        List<Snapshot> snapshots = Lists.newArrayList(icebergTable.snapshots());
        Assertions.assertEquals(1, snapshots.size());

        // create a new tag: tag1
        catalog.createOrReplaceTag(dbName, tblName, info);
        assertSnapshotRef(
                icebergTable.refs().get(tag1),
                icebergTable.currentSnapshot().snapshotId(),
                false, null, null, null);

        // create an existed tag: tag1
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> catalog.createOrReplaceTag(dbName, tblName, info));

        // create an existed tag with replace
        CreateOrReplaceTagInfo info2 =
                new CreateOrReplaceTagInfo(tag1, true, true, false, TagOptions.EMPTY);
        catalog.createOrReplaceTag(dbName, tblName, info2);
        assertSnapshotRef(
                icebergTable.refs().get(tag1),
                icebergTable.currentSnapshot().snapshotId(),
                false, null, null, null);

        // create an existed tag with if not exists
        CreateOrReplaceTagInfo info3 =
                new CreateOrReplaceTagInfo(tag1, true, false, true, TagOptions.EMPTY);
        catalog.createOrReplaceTag(dbName, tblName, info3);
        assertSnapshotRef(
                icebergTable.refs().get(tag1),
                icebergTable.currentSnapshot().snapshotId(),
                false, null, null, null);

        // add some data
        addSomeDataIntoIcebergTable();
        addSomeDataIntoIcebergTable();
        snapshots = Lists.newArrayList(icebergTable.snapshots());
        Assertions.assertEquals(3, snapshots.size());

        // create new tag: tag2 with snapshotId
        TagOptions tagOps = new TagOptions(
                Optional.of(snapshots.get(1).snapshotId()),
                Optional.empty());
        CreateOrReplaceTagInfo info4 =
                new CreateOrReplaceTagInfo(tag2, true, false, false, tagOps);
        catalog.createOrReplaceTag(dbName, tblName, info4);
        assertSnapshotRef(
                icebergTable.refs().get(tag2),
                snapshots.get(1).snapshotId(),
                false, null, null, null);

        // update tag2
        TagOptions tagOps2 = new TagOptions(
                Optional.empty(),
                Optional.of(2L));
        CreateOrReplaceTagInfo info5 =
                new CreateOrReplaceTagInfo(tag2, true, true, false, tagOps2);
        catalog.createOrReplaceTag(dbName, tblName, info5);
        assertSnapshotRef(
                icebergTable.refs().get(tag2),
                icebergTable.currentSnapshot().snapshotId(),
                false, null, null, 2L);

        // create new tag: tag3
        CreateOrReplaceTagInfo info6 =
                new CreateOrReplaceTagInfo(tag3, true, false, false, tagOps2);
        catalog.createOrReplaceTag(dbName, tblName, info6);
        assertSnapshotRef(
                icebergTable.refs().get(tag3),
                icebergTable.currentSnapshot().snapshotId(),
                false, null, null, 2L);

        Assertions.assertEquals(4, icebergTable.refs().size());
    }

    @Test
    public void testCreateBranchWithNotEmptyTable() throws UserException, IOException {

        String branch1 = "branch1";
        String branch2 = "branch2";
        String branch3 = "branch3";

        // create a new branch: branch1
        CreateOrReplaceBranchInfo info =
                new CreateOrReplaceBranchInfo(branch1, true, false, false, BranchOptions.EMPTY);
        catalog.createOrReplaceBranch(dbName, tblName, info);
        List<Snapshot> snapshots = Lists.newArrayList(icebergTable.snapshots());
        Assertions.assertEquals(1, snapshots.size());
        assertSnapshotRef(
                icebergTable.refs().get(branch1),
                snapshots.get(0).snapshotId(),
                true, null, null, null);

        // create an existed branch, failed
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> catalog.createOrReplaceBranch(dbName, tblName, info));

        // create or replace an empty branch, will fail
        // because cannot perform a replace operation on an empty branch.
        CreateOrReplaceBranchInfo info2 =
                new CreateOrReplaceBranchInfo(branch1, true, true, false, BranchOptions.EMPTY);
        Assertions.assertThrows(
                UserException.class,
                () -> catalog.createOrReplaceBranch(dbName, tblName, info2));

        // create an existed branch with ifNotExists
        CreateOrReplaceBranchInfo info4 =
                new CreateOrReplaceBranchInfo(branch1, true, false, true, BranchOptions.EMPTY);
        catalog.createOrReplaceBranch(dbName, tblName, info4);
        assertSnapshotRef(
                icebergTable.refs().get(branch1),
                snapshots.get(0).snapshotId(),
                true, null, null, null);

        // add some data
        addSomeDataIntoIcebergTable();
        snapshots = Lists.newArrayList(icebergTable.snapshots());
        Assertions.assertEquals(2, snapshots.size());

        // update branch1
        catalog.createOrReplaceBranch(dbName, tblName, info2);
        assertSnapshotRef(
                icebergTable.refs().get(branch1),
                icebergTable.currentSnapshot().snapshotId(),
                true, null, null, null);

        // create or replace a new branch: branch2
        CreateOrReplaceBranchInfo info3 =
                new CreateOrReplaceBranchInfo(branch2, true, true, false, BranchOptions.EMPTY);
        catalog.createOrReplaceBranch(dbName, tblName, info3);
        assertSnapshotRef(
                icebergTable.refs().get(branch2),
                icebergTable.currentSnapshot().snapshotId(),
                true, null, null, null);

        // update branch2
        BranchOptions brOps = new BranchOptions(
                Optional.empty(),
                Optional.of(1L),
                Optional.of(2),
                Optional.of(3L));
        CreateOrReplaceBranchInfo info5 =
                new CreateOrReplaceBranchInfo(branch2, true, true, false, brOps);
        catalog.createOrReplaceBranch(dbName, tblName, info5);
        assertSnapshotRef(
                icebergTable.refs().get(branch2),
                icebergTable.currentSnapshot().snapshotId(),
                true, 1L, 2, 3L);

        // total branch:
        //   'main','branch1','branch2'
        Assertions.assertEquals(3, icebergTable.refs().size());

        // insert some data
        addSomeDataIntoIcebergTable();
        addSomeDataIntoIcebergTable();
        addSomeDataIntoIcebergTable();
        addSomeDataIntoIcebergTable();
        snapshots = Lists.newArrayList(icebergTable.snapshots());
        Assertions.assertEquals(6, snapshots.size());

        // create a new branch: branch3
        BranchOptions brOps2 = new BranchOptions(
                Optional.of(snapshots.get(4).snapshotId()),
                Optional.of(1L),
                Optional.of(2),
                Optional.of(3L));
        CreateOrReplaceBranchInfo info6 =
                new CreateOrReplaceBranchInfo(branch3, true, true, false, brOps2);
        catalog.createOrReplaceBranch(dbName, tblName, info6);
        assertSnapshotRef(
                icebergTable.refs().get(branch3),
                snapshots.get(4).snapshotId(),
                true, 1L, 2, 3L);

        // update branch1
        catalog.createOrReplaceBranch(dbName, tblName, info2);
        assertSnapshotRef(
                icebergTable.refs().get(branch1),
                icebergTable.currentSnapshot().snapshotId(),
                true, null, null, null);

        Assertions.assertEquals(4, icebergTable.refs().size());
    }

    private void addSomeDataIntoIcebergTable() throws IOException {
        Path fileA = Files.createFile(tempDirectory.resolve(UUID.randomUUID().toString()));
        DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                .withPath(fileA.toString())
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withFormat("parquet");
        icebergTable.newFastAppend()
                .appendFile(builder.build())
                .commit();
    }

    private void assertSnapshotRef(
            SnapshotRef ref,
            Long snapshotId,
            boolean isBranch,
            Long maxSnapshotAgeMs,
            Integer minSnapshotsToKeep,
            Long maxRefAgeMs) {
        if (snapshotId != null) {
            Assertions.assertEquals(snapshotId, ref.snapshotId());
        }
        if (isBranch) {
            Assertions.assertTrue(ref.isBranch());
        } else {
            Assertions.assertTrue(ref.isTag());
        }
        Assertions.assertEquals(maxSnapshotAgeMs, ref.maxSnapshotAgeMs());
        Assertions.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
        Assertions.assertEquals(maxRefAgeMs, ref.maxRefAgeMs());
    }
}
