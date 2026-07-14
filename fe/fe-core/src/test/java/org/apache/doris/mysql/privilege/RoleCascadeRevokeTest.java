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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RoleCascadeRevokeTest {

    @Test
    public void testRemoveCatalogPrivs() throws AnalysisException {
        Role role = new Role("test_role");
        TablePattern catalogPat = new TablePattern("ctl1", "*", "*");
        catalogPat.analyze();
        TablePattern dbPat = new TablePattern("ctl1", "db1", "*");
        dbPat.analyze();
        TablePattern tblPat = new TablePattern("ctl1", "db1", "tbl1");
        tblPat.analyze();
        TablePattern otherCtlPat = new TablePattern("ctl2", "db2", "*");
        otherCtlPat.analyze();

        PrivBitSet selectPriv = PrivBitSet.of(Privilege.SELECT_PRIV);
        PrivBitSet alterPriv = PrivBitSet.of(Privilege.ALTER_PRIV);

        role.getTblPatternToPrivs().put(catalogPat, selectPriv);
        role.getTblPatternToPrivs().put(dbPat, alterPriv);
        role.getTblPatternToPrivs().put(tblPat, selectPriv);
        role.getTblPatternToPrivs().put(otherCtlPat, alterPriv);

        List<Map.Entry<TablePattern, PrivBitSet>> removed = role.removeCatalogPrivs("ctl1");
        Assert.assertEquals(3, removed.size());
        Assert.assertEquals(1, role.getTblPatternToPrivs().size());
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(otherCtlPat));
    }

    @Test
    public void testRemoveDbPrivs() throws AnalysisException {
        Role role = new Role("test_role");
        TablePattern catalogPat = new TablePattern("internal", "*", "*");
        catalogPat.analyze();
        TablePattern dbPat = new TablePattern("internal", "db1", "*");
        dbPat.analyze();
        TablePattern tblPat = new TablePattern("internal", "db1", "tbl1");
        tblPat.analyze();
        TablePattern otherDbPat = new TablePattern("internal", "db2", "*");
        otherDbPat.analyze();

        PrivBitSet selectPriv = PrivBitSet.of(Privilege.SELECT_PRIV);
        PrivBitSet alterPriv = PrivBitSet.of(Privilege.ALTER_PRIV);

        role.getTblPatternToPrivs().put(catalogPat, selectPriv);
        role.getTblPatternToPrivs().put(dbPat, alterPriv);
        role.getTblPatternToPrivs().put(tblPat, selectPriv);
        role.getTblPatternToPrivs().put(otherDbPat, alterPriv);

        List<Map.Entry<TablePattern, PrivBitSet>> removed = role.removeDbPrivs("internal", "db1");
        Assert.assertEquals(2, removed.size());
        Assert.assertEquals(2, role.getTblPatternToPrivs().size());
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(catalogPat));
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(otherDbPat));
    }

    @Test
    public void testRemoveTablePrivs() throws AnalysisException {
        Role role = new Role("test_role");
        TablePattern dbPat = new TablePattern("internal", "db1", "*");
        dbPat.analyze();
        TablePattern tbl1Pat = new TablePattern("internal", "db1", "tbl1");
        tbl1Pat.analyze();
        TablePattern tbl2Pat = new TablePattern("internal", "db1", "tbl2");
        tbl2Pat.analyze();

        PrivBitSet selectPriv = PrivBitSet.of(Privilege.SELECT_PRIV);
        PrivBitSet alterPriv = PrivBitSet.of(Privilege.ALTER_PRIV);

        role.getTblPatternToPrivs().put(dbPat, selectPriv);
        role.getTblPatternToPrivs().put(tbl1Pat, alterPriv);
        role.getTblPatternToPrivs().put(tbl2Pat, selectPriv);

        List<Map.Entry<TablePattern, PrivBitSet>> removed = role.removeTablePrivs("internal", "db1", "tbl1");
        Assert.assertEquals(1, removed.size());
        Assert.assertEquals(2, role.getTblPatternToPrivs().size());
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(dbPat));
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(tbl2Pat));
    }

    @Test
    public void testRemoveResourcePrivs() throws AnalysisException {
        Role role = new Role("test_role");
        ResourcePattern res1 = new ResourcePattern("res1", ResourceTypeEnum.GENERAL);
        ResourcePattern res2 = new ResourcePattern("res2", ResourceTypeEnum.GENERAL);

        PrivBitSet usagePriv = PrivBitSet.of(Privilege.USAGE_PRIV);

        role.getResourcePatternToPrivs().put(res1, usagePriv);
        role.getResourcePatternToPrivs().put(res2, usagePriv);

        List<Map.Entry<ResourcePattern, PrivBitSet>> removed = role.removeResourcePrivs("res1");
        Assert.assertEquals(1, removed.size());
        Assert.assertEquals(1, role.getResourcePatternToPrivs().size());
        Assert.assertTrue(role.getResourcePatternToPrivs().containsKey(res2));
    }

    @Test
    public void testRemoveWorkloadGroupPrivs() throws AnalysisException {
        Role role = new Role("test_role");
        WorkloadGroupPattern wg1 = new WorkloadGroupPattern("wg1");
        WorkloadGroupPattern wg2 = new WorkloadGroupPattern("wg2");

        PrivBitSet usagePriv = PrivBitSet.of(Privilege.USAGE_PRIV);

        role.getWorkloadGroupPatternToPrivs().put(wg1, usagePriv);
        role.getWorkloadGroupPatternToPrivs().put(wg2, usagePriv);

        List<Map.Entry<WorkloadGroupPattern, PrivBitSet>> removed = role.removeWorkloadGroupPrivs("wg1");
        Assert.assertEquals(1, removed.size());
        Assert.assertEquals(1, role.getWorkloadGroupPatternToPrivs().size());
        Assert.assertTrue(role.getWorkloadGroupPatternToPrivs().containsKey(wg2));
    }

    @Test
    public void testRemoveNonExistentPrivs() {
        Role role = new Role("test_role");
        List<Map.Entry<TablePattern, PrivBitSet>> dbRemoved = role.removeDbPrivs("internal", "nonexistent");
        Assert.assertTrue(dbRemoved.isEmpty());

        List<Map.Entry<ResourcePattern, PrivBitSet>> resRemoved = role.removeResourcePrivs("nonexistent");
        Assert.assertTrue(resRemoved.isEmpty());

        List<Map.Entry<WorkloadGroupPattern, PrivBitSet>> wgRemoved = role.removeWorkloadGroupPrivs("nonexistent");
        Assert.assertTrue(wgRemoved.isEmpty());
    }

    @Test
    public void testRemoveDbPrivsPreservesCatalogLevel() throws AnalysisException {
        Role role = new Role("test_role");
        TablePattern catalogPat = new TablePattern("internal", "*", "*");
        catalogPat.analyze();
        TablePattern dbPat = new TablePattern("internal", "db1", "*");
        dbPat.analyze();

        PrivBitSet selectPriv = PrivBitSet.of(Privilege.SELECT_PRIV);
        PrivBitSet alterPriv = PrivBitSet.of(Privilege.ALTER_PRIV);

        role.getTblPatternToPrivs().put(catalogPat, selectPriv);
        role.getTblPatternToPrivs().put(dbPat, alterPriv);

        List<Map.Entry<TablePattern, PrivBitSet>> removed = role.removeDbPrivs("internal", "db1");
        Assert.assertEquals(1, removed.size());
        Assert.assertEquals(1, role.getTblPatternToPrivs().size());
        Assert.assertTrue(role.getTblPatternToPrivs().containsKey(catalogPat));
    }

    @Test
    public void testRemoveCatalogPrivsReturnsCorrectEntries() throws AnalysisException {
        Role role = new Role("test_role");
        TablePattern catalogPat = new TablePattern("ctl1", "*", "*");
        catalogPat.analyze();
        TablePattern dbPat = new TablePattern("ctl1", "db1", "*");
        dbPat.analyze();

        PrivBitSet selectPriv = PrivBitSet.of(Privilege.SELECT_PRIV);
        PrivBitSet alterPriv = PrivBitSet.of(Privilege.ALTER_PRIV);

        role.getTblPatternToPrivs().put(catalogPat, selectPriv);
        role.getTblPatternToPrivs().put(dbPat, alterPriv);

        List<Map.Entry<TablePattern, PrivBitSet>> removed = role.removeCatalogPrivs("ctl1");
        Assert.assertEquals(2, removed.size());
        for (Map.Entry<TablePattern, PrivBitSet> entry : removed) {
            Assert.assertEquals("ctl1", entry.getKey().getQualifiedCtl());
        }
    }
}
