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

package org.apache.doris.backup;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.RandomDistributionDesc;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.analysis.TableName;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterClauseRWTest {

    @Before
    public void setUp() {
        System.out.println(AddPartitionClause.class.getName());
        System.out.println(AddPartitionClause.class.getCanonicalName());
    }

    @Test
    public void test() throws FileNotFoundException {
        List<AlterClause> clauses = Lists.newArrayList();
        AlterTableStmt stmt = new AlterTableStmt(new TableName("db", "tbl"), clauses);
        
        
        File file = new File("./addPartitionClause");
        try {
            file.createNewFile();
            DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

            // add partition clause
            String partititionName = "p1";
            List<String> values = Lists.newArrayList("100");
            PartitionKeyDesc keyDesc = new PartitionKeyDesc(values);
            Map<String, String> properties = Maps.newHashMap();
            SingleRangePartitionDesc partitionDesc = new SingleRangePartitionDesc(false, partititionName, keyDesc,
                                                                                  properties);
            DistributionDesc distributionDesc = new RandomDistributionDesc(32);
            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, distributionDesc, properties);
            clauses.add(addPartitionClause);
            
            // add rollup clause
            AddRollupClause rollupClause = new AddRollupClause("rollup", Lists.newArrayList("k1", "v1"), 
                                                               null, null, properties);
            clauses.add(rollupClause);

            // write
            stmt.write(out);
            out.flush();
            out.close();

            System.out.println(stmt.toSql());

            // read
            DataInputStream in = new DataInputStream(new FileInputStream(file));
            stmt = new AlterTableStmt();
            stmt.readFields(in);
            System.out.println(stmt.toSql());

            in.close();
            file.delete();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
