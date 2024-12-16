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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.meta.MetaContext;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class VirtualSlotRefTest {
    private Analyzer analyzer;
    private List<Expr> slots;
    private TupleDescriptor virtualTuple;
    private VirtualSlotRef virtualSlot;

    @Before
    public void setUp() throws IOException, AnalysisException {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        // read objects from file
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        analyzer = new Analyzer(analyzerBase.getEnv(), analyzerBase.getContext());
        String[] cols = {"k1", "k2", "k3"};
        slots = new ArrayList<>();
        for (String col : cols) {
            SlotRef expr = new SlotRef(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "testdb", "t"), col);
            slots.add(expr);
        }
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            TableName tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "testdb", "t");
            tableName.analyze(analyzerBase);
            td.setTable(analyzerBase.getTableOrAnalysisException(tableName));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        virtualTuple = analyzer.getDescTbl().createTupleDescriptor("VIRTUAL_TUPLE");
        virtualSlot = new VirtualSlotRef("colName", Type.BIGINT, virtualTuple, slots);
    }

    @Test
    public void testClone() {
        Expr v = virtualSlot.clone();
        Assert.assertTrue(v instanceof VirtualSlotRef);
        Assert.assertEquals(((VirtualSlotRef) v).getRealSlots().get(0), virtualSlot.getRealSlots().get(0));
        Assert.assertFalse(((VirtualSlotRef) v).getRealSlots().get(0) == virtualSlot.getRealSlots().get(0));
    }

    @Test
    public void analyzeImpl() {
        try {
            virtualSlot.analyzeImpl(analyzer);
        } catch (Exception e) {
            Assert.fail("analyze throw exception");
        }
    }
}
