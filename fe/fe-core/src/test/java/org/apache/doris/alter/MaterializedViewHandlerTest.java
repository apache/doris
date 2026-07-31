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

package org.apache.doris.alter;

import org.apache.doris.analysis.MVColumnItem;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class MaterializedViewHandlerTest {
    @Test
    public void testDifferentBaseTable() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        Database db = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(createMaterializedViewCommand.getBaseIndexName()).thenReturn("t1");
        Mockito.when(olapTable.getName()).thenReturn("t2");
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewCommand,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testNotNormalTable() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        Database db = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        final String baseIndexName = "t1";
        Mockito.when(createMaterializedViewCommand.getBaseIndexName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getState()).thenReturn(OlapTable.OlapTableState.ROLLUP);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewCommand,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testErrorBaseIndexName() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        Database db = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        final String baseIndexName = "t1";
        Mockito.when(createMaterializedViewCommand.getBaseIndexName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        Mockito.when(olapTable.getIndexIdByName(baseIndexName)).thenReturn(null);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewCommand, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testRollupReplica() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        Database db = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex materializedIndex = Mockito.mock(MaterializedIndex.class);
        final String baseIndexName = "t1";
        final Long baseIndexId = new Long(1);
        Mockito.when(createMaterializedViewCommand.getBaseIndexName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getName()).thenReturn(baseIndexName);
        Mockito.when(olapTable.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        Mockito.when(olapTable.getIndexIdByName(baseIndexName)).thenReturn(baseIndexId);
        Mockito.when(olapTable.getPartitions()).thenReturn(Lists.newArrayList(partition));
        Mockito.when(partition.getIndex(baseIndexId)).thenReturn(materializedIndex);
        Mockito.when(materializedIndex.getState()).thenReturn(MaterializedIndex.IndexState.SHADOW);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewCommand, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateMVName() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        final String mvName = "mv1";
        Mockito.when(olapTable.hasMaterializedIndex(mvName)).thenReturn(true);
        Mockito.when(createMaterializedViewCommand.getMVName()).thenReturn(mvName);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewCommand, olapTable, new HashMap<String, String>());
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testInvalidKeysType() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getRowStoreCol()).thenReturn(null);
        Mockito.when(olapTable.getKeysType()).thenReturn(KeysType.AGG_KEYS);

        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewCommand, olapTable, new HashMap<String, String>());
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateTable() {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        final String mvName = "mv1";
        final String columnName1 = "k1";
        SlotRef slot = new SlotRef(Type.VARCHAR, false);
        slot.setCol(columnName1);
        slot.setLabel(columnName1);

        MVColumnItem mvColumnItem = null;
        try {
            mvColumnItem = new MVColumnItem(slot);
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
        mvColumnItem.setIsKey(true);
        mvColumnItem.setAggregationType(null, false);
        mvColumnItem.getBaseColumnNames().add(columnName1);
        List<MVColumnItem> list = Lists.newArrayList(mvColumnItem);
        Mockito.when(olapTable.getBaseColumn(columnName1)).thenReturn(null);
        Mockito.when(olapTable.hasMaterializedIndex(mvName)).thenReturn(false);
        Mockito.when(createMaterializedViewCommand.getMVName()).thenReturn(mvName);
        Mockito.when(createMaterializedViewCommand.getMVColumnItemList()).thenReturn(list);
        Mockito.when(olapTable.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(olapTable.getRowStoreCol()).thenReturn(null);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            List<Column> mvColumns = Deencapsulation.invoke(materializedViewHandler,
                    "checkAndPrepareMaterializedView",
                    createMaterializedViewCommand, olapTable, new HashMap<String, String>());
            Assert.assertEquals(1, mvColumns.size());
            Column newMVColumn = mvColumns.get(0);
            Assert.assertEquals(columnName1, newMVColumn.getName());
            Assert.assertTrue(newMVColumn.isKey());
            Assert.assertEquals(null, newMVColumn.getAggregationType());
            Assert.assertEquals(false, newMVColumn.isAggregationTypeImplicit());
            Assert.assertEquals(Type.VARCHAR.getPrimitiveType(), newMVColumn.getType().getPrimitiveType());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Disabled
    public void checkInvalidPartitionKeyMV() throws DdlException {
        CreateMaterializedViewCommand createMaterializedViewCommand = Mockito.mock(CreateMaterializedViewCommand.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        final String mvName = "mv1";
        final String columnName1 = "k1";

        SlotRef slot = new SlotRef(Type.VARCHAR, false);
        slot.setCol(columnName1);
        slot.setLabel(columnName1);

        MVColumnItem mvColumnItem = null;
        try {
            mvColumnItem = new MVColumnItem(slot);
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        mvColumnItem.setIsKey(false);
        mvColumnItem.setAggregationType(AggregateType.SUM, false);
        List<MVColumnItem> list = Lists.newArrayList(mvColumnItem);
        Set<String> partitionColumnNames = Sets.newHashSet();
        partitionColumnNames.add(columnName1);
        Mockito.when(olapTable.hasMaterializedIndex(mvName)).thenReturn(false);
        Mockito.when(createMaterializedViewCommand.getMVName()).thenReturn(mvName);
        Mockito.when(createMaterializedViewCommand.getMVColumnItemList()).thenReturn(list);
        Mockito.when(olapTable.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(olapTable.getPartitionColumnNames()).thenReturn(partitionColumnNames);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewCommand, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testCheckDropMaterializedView() {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex materializedIndex = Mockito.mock(MaterializedIndex.class);
        String mvName = "mv_1";
        Mockito.when(olapTable.getName()).thenReturn("table1");
        Mockito.when(olapTable.hasMaterializedIndex(mvName)).thenReturn(true);
        Mockito.when(olapTable.getIndexIdByName(mvName)).thenReturn(1L);
        Mockito.when(olapTable.getSchemaHashByIndexId(1L)).thenReturn(1);
        Mockito.when(olapTable.getPartitions()).thenReturn(Lists.newArrayList(partition));
        Mockito.when(partition.getIndex(1L)).thenReturn(materializedIndex);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkDropMaterializedView", mvName, olapTable);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

}
