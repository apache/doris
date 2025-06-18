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

package org.apache.doris.datasource.tvf;

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.common.FeConstants;
import org.apache.doris.tablefunction.FileTableValuedFunction;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FileTableValuedFunctionTest extends TestWithFeService {

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testS3TableValuedFunction() throws Exception {
        // Test S3 TVF with old style properties
        String queryOld = "select * from file(\n"
                + "  'uri' = 'http://s3.us-east-1.amazonaws.com/my-bucket/test.parquet',\n"
                + "  'access_key' = 'akk',\n"
                + "  'secret_key' = 'skk',\n"
                + "  'region' = 'us-east-1',\n"
                + "  'format' = 'parquet',\n"
                + "  'use_path_style' = 'true'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(queryOld);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        TableValuedFunctionRef funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        FileTableValuedFunction tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("S3TableValuedFunction", tvf.getTableName());
        Assertions.assertEquals(TFileType.FILE_S3, tvf.getTFileType());

        // Test S3 TVF with new style properties
        String queryNew = "select * from file(\n"
                + "  'uri' = 's3://bucket/key',\n"
                + "  's3.access_key' = 'ak',\n"
                + "  's3.secret_key' = 'sk',\n"
                + "  's3.region' = 'us-east-1',\n"
                + "  's3.endpoint' = 'endpoint',\n"
                + "  'format' = 'csv'\n"
                + ") limit 10;";
        analyzedStmt = createStmt(queryNew);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("S3TableValuedFunction", tvf.getTableName());
        Assertions.assertEquals(TFileType.FILE_S3, tvf.getTFileType());
    }

    @Test
    public void testHdfsTableValuedFunction() throws Exception {
        // Test HDFS TVF with basic properties
        String query = "select * from file(\n"
                + "  'uri' = 'hdfs://namenode/path/to/file.csv',\n"
                + "  'hadoop.username' = 'hadoop',\n"
                + "  'format' = 'csv'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(query);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        TableValuedFunctionRef funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        FileTableValuedFunction tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("HDFSTableValuedFunction", tvf.getTableName());
        Assertions.assertEquals(TFileType.FILE_HDFS, tvf.getTFileType());

        // Test HDFS TVF with additional properties
        String queryWithProps = "select * from file(\n"
                + "  'uri' = 'hdfs://namenode/path/to/file.csv',\n"
                + "  'hadoop.username' = 'hadoop',\n"
                + "  'hadoop.fs.name' = 'hdfs://namenode:8020',\n"
                + "  'format' = 'csv'\n"
                + ") limit 10;";
        analyzedStmt = createStmt(queryWithProps);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("HDFSTableValuedFunction", tvf.getTableName());
    }

    @Test
    public void testS3PropertiesConversion() throws Exception {
        // Test old style properties conversion
        String queryOld = "select * from file(\n"
                + "  'uri' = 's3://bucket/key',\n"
                + "  's3.access_key' = 'ak',\n"
                + "  's3.secret_key' = 'sk',\n"
                + "  's3.region' = 'us-east-1',\n"
                + "  's3.endpoint' = 'endpoint',\n"
                + "  'format' = 'csv'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(queryOld);
        TableValuedFunctionRef funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        FileTableValuedFunction tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("S3TableValuedFunction", tvf.getTableName());
        Assertions.assertEquals(TFileType.FILE_S3, tvf.getTFileType());
    }

    @Test
    public void testHdfsPropertiesConversion() throws Exception {
        String query = "select * from file(\n"
                + "  'uri' = 'hdfs://namenode/path/to/file.csv',\n"
                + "  'hadoop.username' = 'hadoop',\n"
                + "  'hadoop.fs.name' = 'hdfs://namenode:8020',\n"
                + "  'format' = 'csv'\n"
                + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(query);
        TableValuedFunctionRef funcTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        FileTableValuedFunction tvf = (FileTableValuedFunction) funcTable.getTableFunction();
        Assertions.assertEquals("HDFSTableValuedFunction", tvf.getTableName());
        Assertions.assertEquals(TFileType.FILE_HDFS, tvf.getTFileType());
    }
}
