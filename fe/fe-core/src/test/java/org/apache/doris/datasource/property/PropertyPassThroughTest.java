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

package org.apache.doris.datasource.property;

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.common.FeConstants;
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PropertyPassThroughTest extends TestWithFeService  {
    @Test
    public void testS3TVFPropertiesPassThrough() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryOld = "select * from s3(\n"
                + "  'uri' = 'http://s3.us-east-1.amazonaws.com/my-bucket/test.parquet',\n"
                + "  'access_key' = 'akk',\n"
                + "  'secret_key' = 'skk',\n"
                + "  'region' = 'us-east-1',\n"
                + "  'format' = 'parquet',\n"
                + "  'fs.s3a.list.version' = '1',\n"
                + "  'test_property' = 'test',\n"
                + "  'use_path_style' = 'true'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(queryOld);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        TableValuedFunctionRef oldFuncTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        S3TableValuedFunction s3Tvf = (S3TableValuedFunction) oldFuncTable.getTableFunction();
        Assertions.assertTrue(s3Tvf.getBrokerDesc().getProperties().containsKey("fs.s3a.list.version"));
        Assertions.assertTrue(s3Tvf.getBrokerDesc().getProperties().containsKey("test_property"));
    }

    @Test
    public void testHdfsTVFPropertiesPassThrough() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryOld = "select * from hdfs(\n"
                + "  'uri' = 'hdfs://HDFS11111871/path/example_table/country=USA/city=NewYork/000000_0',\n"
                + "  'hadoop.username' = 'hadoop',\n"
                + "  'path_partition_keys' = 'country,city',\n"
                + "  'format' = 'orc',\n"
                + "  'test_property' = 'test'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(queryOld);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        TableValuedFunctionRef oldFuncTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        HdfsTableValuedFunction hdfsTvf = (HdfsTableValuedFunction) oldFuncTable.getTableFunction();
        Assertions.assertTrue(hdfsTvf.getBrokerDesc().getProperties().containsKey("test_property"));
    }
}
