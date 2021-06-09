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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class OlapScanNodeTest {    
    // columnA in (1) hashmode=3
    @Test
    public void testHashDistributionOneUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));
        
        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));

        Expr compareExpr = new SlotRef(new TableName("db", "tableName"), "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);
        
        DistributionPruner partitionPruner  = new HashDistributionPruner(
                partitions, 
                columns,
                filterMap,
                3);
        
        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 1);

        for (Long id : ids) {
            Assert.assertEquals((1 & 0xffffffff) % 3, id.intValue());
        }
    }
     
    // columnA in (1, 2 ,3, 4, 5, 6) hashmode=3
    @Test
    public void testHashPartitionManyUser() throws AnalysisException {

        List<Long> partitions = new ArrayList<>();
        partitions.add(new Long(0));
        partitions.add(new Long(1));
        partitions.add(new Long(2));

        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("columnA", PrimitiveType.BIGINT));        
                
        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(1));
        inList.add(new IntLiteral(2));
        inList.add(new IntLiteral(3));
        inList.add(new IntLiteral(4));
        inList.add(new IntLiteral(5));
        inList.add(new IntLiteral(6));

        Expr compareExpr = new SlotRef(new TableName("db", "tableName"), "columnA");
        InPredicate inPredicate = new InPredicate(compareExpr, inList, false);

        PartitionColumnFilter  columnFilter = new PartitionColumnFilter();
        columnFilter.setInPredicate(inPredicate);
        Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
        filterMap.put("columnA", columnFilter);
 
        DistributionPruner partitionPruner  = new HashDistributionPruner(
                partitions, 
                columns,
                filterMap,
                3);
        
        Collection<Long> ids = partitionPruner.prune();
        Assert.assertEquals(ids.size(), 3);
    }
    
    @Test
    public void testHashForIntLiteral() {
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(1), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        }
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(2), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        }
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(3), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 0);
        }
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(4), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 1);
        }
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(5), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        }
        {
            PartitionKey hashKey = new PartitionKey();
            hashKey.pushColumn(new IntLiteral(6), PrimitiveType.BIGINT);
            long hashValue = hashKey.getHashValue();
            long mod = (int) ((hashValue & 0xffffffff) % 3);
            Assert.assertEquals(mod, 2);
        }
    }

//    @Test
//    public void testConstructInputPartitionByDistributionInfo(@Injectable OlapTable olapTable,
//                                                              @Injectable TupleDescriptor tupleDescriptor) {
//        PlanNodeId planNodeId = new PlanNodeId(1);
//        OlapScanNode olapScanNode = new OlapScanNode(planNodeId, tupleDescriptor, "scan node");
//        Deencapsulation.setField(olapScanNode, "olapTable", olapTable);
//        new Expectations() {
//            {
//                olapTable.getDefaultDistributionInfo();
//                result =
//            }
//        };
//        olapScanNode.constructInputPartitionByDistributionInfo();
//    }
}
