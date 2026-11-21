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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BaseTableInfoTest {

    @Test
    public void testCompatibleReturnsFalseWhenCtlNameAlreadyPopulated() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getName()).thenReturn("mv1");
        Mockito.when(table.getDBName()).thenReturn("db1");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);

        BaseTableInfo info = new BaseTableInfo(table, 200L);
        Assertions.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, info.getCtlName());
        Assertions.assertFalse(info.compatible(catalogMgr),
                "compatible must be no-op when ctlName is present");
    }

    @Test
    public void testMTMVRelationCompatibleReturnsFalseWhenEverythingIsAlreadyPostName() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getName()).thenReturn("mv1");
        Mockito.when(table.getDBName()).thenReturn("db1");
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);

        BaseTableInfo t1 = new BaseTableInfo(table, 200L);
        BaseTableInfo t2 = new BaseTableInfo(table, 200L);
        BaseTableInfo t3 = new BaseTableInfo(table, 200L);
        MTMVRelation relation = new MTMVRelation(Sets.newHashSet(t1), Sets.newHashSet(t2), Sets.newHashSet(t3),
                Sets.newHashSet(), Sets.newHashSet());
        Assertions.assertFalse(relation.compatible(catalogMgr),
                "MTMVRelation.compatible must return false when every BaseTableInfo already carries ctlName");
    }
}
