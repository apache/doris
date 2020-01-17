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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class VirtualSlotRefTest {
    private Analyzer analyzer;
    private List<Expr> slots;
    private TupleDescriptor virtualTuple;
    private VirtualSlotRef virtualSlot;
    DataOutputStream dos;
    File file;
    DataInputStream dis;

    @Before
    public void setUp() throws IOException {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        analyzer = new Analyzer(analyzerBase.getCatalog(), analyzerBase.getContext());
        String[] cols = {"k1", "k2", "k3"};
        slots = new ArrayList<>();
        for (String col : cols) {
            SlotRef expr = new SlotRef(new TableName("testdb", "t"), col);
            slots.add(expr);
        }
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            td.setTable(analyzerBase.getTable(new TableName("testdb", "t")));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        virtualTuple = analyzer.getDescTbl().createTupleDescriptor("VIRTUAL_TUPLE");
        virtualSlot = new VirtualSlotRef("colName", Type.BIGINT, virtualTuple, slots);
        file = new File("./virtualSlot");
        file.createNewFile();
        dos = new DataOutputStream(new FileOutputStream(file));
        dis = new DataInputStream(new FileInputStream(file));
    }

    @After
    public void tearDown() throws Exception {
        dis.close();
        dos.close();
        file.delete();
    }

    @Test
    public void read() throws IOException {
        virtualSlot.write(dos);
        virtualSlot.setRealSlots(slots);
        VirtualSlotRef v = VirtualSlotRef.read(dis);
        Assert.assertEquals(3, v.getRealSlots().size());
    }

    @Test
    public void testClone() {
        Expr v = virtualSlot.clone();
        Assert.assertTrue(v instanceof VirtualSlotRef);
        Assert.assertTrue(((VirtualSlotRef) v).getRealSlots().get(0).equals(virtualSlot.getRealSlots().get(0)));
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
