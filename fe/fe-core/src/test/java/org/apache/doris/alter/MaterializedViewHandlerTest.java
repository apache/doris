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

import org.apache.doris.analysis.CreateMaterializedViewStmt;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

import java.util.List;
import java.util.Set;

public class MaterializedViewHandlerTest {
    @Test
    public void testDifferentBaseTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                       @Injectable Database db,
                                       @Injectable OlapTable olapTable) {
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = "t1";
                olapTable.getName();
                result = "t2";
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewStmt,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testNotNormalTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                   @Injectable Database db,
                                   @Injectable OlapTable olapTable) {
        final String baseIndexName = "t1";
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.ROLLUP;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView", createMaterializedViewStmt,
                    db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testErrorBaseIndexName(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                       @Injectable Database db,
                                       @Injectable OlapTable olapTable) {
        final String baseIndexName = "t1";
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getIndexIdByName(baseIndexName);
                result = null;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testRollupReplica(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                  @Injectable Database db,
                                  @Injectable OlapTable olapTable,
                                  @Injectable Partition partition,
                                  @Injectable MaterializedIndex materializedIndex) {
        final String baseIndexName = "t1";
        final Long baseIndexId = new Long(1);
        new Expectations() {
            {
                createMaterializedViewStmt.getBaseIndexName();
                result = baseIndexName;
                olapTable.getName();
                result = baseIndexName;
                olapTable.getState();
                result = OlapTable.OlapTableState.NORMAL;
                olapTable.getIndexIdByName(baseIndexName);
                result = baseIndexId;
                olapTable.getPartitions();
                result = Lists.newArrayList(partition);
                partition.getIndex(baseIndexId);
                result = materializedIndex;
                materializedIndex.getState();
                result = MaterializedIndex.IndexState.SHADOW;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "processCreateMaterializedView",
                    createMaterializedViewStmt, db, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateMVName(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                    @Injectable OlapTable olapTable) {
        final String mvName = "mv1";
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = true;
                createMaterializedViewStmt.getMVName();
                result = mvName;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testInvalidKeysType(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                    @Injectable OlapTable olapTable) {
        new Expectations() {
            {
                olapTable.getRowStoreCol();
                result = null;
                olapTable.getKeysType();
                result = KeysType.AGG_KEYS;
            }
        };

        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testDuplicateTable(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                   @Injectable OlapTable olapTable) {
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
        List<MVColumnItem> list = Lists.newArrayList(mvColumnItem);
        new Expectations() {
            {
                olapTable.getBaseColumn(CreateMaterializedViewStmt.mvColumnBuilder(columnName1));
                result = null;
                olapTable.hasMaterializedIndex(mvName);
                result = false;
                createMaterializedViewStmt.getMVName();
                result = mvName;
                createMaterializedViewStmt.getMVColumnItemList();
                result = list;
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                olapTable.getRowStoreCol();
                result = null;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            List<Column> mvColumns = Deencapsulation.invoke(materializedViewHandler,
                    "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, olapTable);
            Assert.assertEquals(1, mvColumns.size());
            Column newMVColumn = mvColumns.get(0);
            Assert.assertEquals(CreateMaterializedViewStmt.mvColumnBuilder(columnName1), newMVColumn.getName());
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
    public void checkInvalidPartitionKeyMV(@Injectable CreateMaterializedViewStmt createMaterializedViewStmt,
                                           @Injectable OlapTable olapTable) throws DdlException {
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
        new Expectations() {
            {
                olapTable.hasMaterializedIndex(mvName);
                result = false;
                createMaterializedViewStmt.getMVName();
                result = mvName;
                createMaterializedViewStmt.getMVColumnItemList();
                result = list;
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                olapTable.getPartitionColumnNames();
                result = partitionColumnNames;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkAndPrepareMaterializedView",
                    createMaterializedViewStmt, olapTable);
            Assert.fail();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }

    @Test
    public void testCheckDropMaterializedView(@Injectable OlapTable olapTable, @Injectable Partition partition,
                                              @Injectable MaterializedIndex materializedIndex) {
        String mvName = "mv_1";
        new Expectations() {
            {
                olapTable.getName();
                result = "table1";
                olapTable.hasMaterializedIndex(mvName);
                result = true;
                olapTable.getIndexIdByName(mvName);
                result = 1L;
                olapTable.getSchemaHashByIndexId(1L);
                result = 1;
                olapTable.getPartitions();
                result = Lists.newArrayList(partition);
                partition.getIndex(1L);
                result = materializedIndex;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        try {
            Deencapsulation.invoke(materializedViewHandler, "checkDropMaterializedView", mvName, olapTable);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

}
