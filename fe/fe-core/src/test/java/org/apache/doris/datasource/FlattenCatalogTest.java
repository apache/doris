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

package org.apache.doris.datasource;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TableName;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class FlattenCatalogTest {

    private static final String CTL = "hive";
    private static final String DB = "db1";
    private static final String TBL = "tbl1";

    @Test
    protected void testTableName(@Mocked Analyzer analyzer) throws Exception {
        Config.enable_multi_catalog = true;
        FeConstants.runningUnitTest = true;

        new Expectations() {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;
                analyzer.getDefaultDb();
                minTimes = 0;
                result = DB;
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;
            }
        };

        // tbl
        TableName name = new TableName(null, null, TBL);
        name.analyze(analyzer);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, name.getCtl());
        Assert.assertEquals(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, DB), name.getDb());
        Assert.assertEquals(TBL, name.getTbl());

        // db.tbl
        name = new TableName(null, DB, TBL);
        name.analyze(analyzer);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, name.getCtl());
        Assert.assertEquals(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, DB), name.getDb());
        Assert.assertEquals(TBL, name.getTbl());

        name = new TableName(null, CatalogFlattenUtils.flatten(CTL, DB), TBL);
        name.analyze(analyzer);
        Assert.assertEquals(CTL, name.getCtl());
        Assert.assertEquals(DB, name.getDb());
        Assert.assertEquals(TBL, name.getTbl());

        name = new TableName(CTL, DB, TBL);
        name.analyze(analyzer);
        Assert.assertEquals(CTL, name.getCtl());
        Assert.assertEquals(DB, name.getDb());
        Assert.assertEquals(TBL, name.getTbl());
    }

    @Test
    protected void testFlattenUtil(@Mocked ConnectContext ctx) throws Exception {
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
                ctx.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        };

        String name = "db1";
        Pair<String, String> res = CatalogFlattenUtils.analyzeFlattenName(name);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, res.first);
        Assert.assertEquals("db1", res.second);

        name = "__hive__db1";
        res = CatalogFlattenUtils.analyzeFlattenName(name);
        Assert.assertEquals("hive", res.first);
        Assert.assertEquals("db1", res.second);

        String name2 = "__db1";
        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class, "invalid flatten name", ()->{CatalogFlattenUtils.analyzeFlattenName(name2);});

        String name3 = "__hive_db1";
        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class, "invalid flatten name", ()->{CatalogFlattenUtils.analyzeFlattenName(name3);});
    }
}
