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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ExternalFileTableValuedFunctionTest {
    @Test
    public void testCsvSchemaParse() {
        Config.enable_date_conversion = true;
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:bool;"
                        + "k8:char(10);k9:varchar(20);k10:date;k11:datetime;k12:decimal(10,2)");
        List<Column> csvSchema = Lists.newArrayList();
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assert.fail();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("unsupported column type: bool"));
        }

        csvSchema.clear();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:boolean;"
                        + "k8:string;k9:date;k10:datetime;k11:decimal(10, 2);k12:decimal( 38,10); k13:datetime(5)");
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assert.assertEquals(13, csvSchema.size());
            Column decimalCol = csvSchema.get(10);
            Assert.assertEquals(10, decimalCol.getPrecision());
            Assert.assertEquals(2, decimalCol.getScale());
            decimalCol = csvSchema.get(11);
            Assert.assertEquals(38, decimalCol.getPrecision());
            Assert.assertEquals(10, decimalCol.getScale());
            Column datetimeCol = csvSchema.get(12);
            Assert.assertEquals(5, datetimeCol.getScale());

            for (int i = 0; i < csvSchema.size(); i++) {
                Column col = csvSchema.get(i);
                switch (col.getName()) {
                    case "k1":
                        Assert.assertEquals(PrimitiveType.INT, col.getType().getPrimitiveType());
                        break;
                    case "k2":
                        Assert.assertEquals(PrimitiveType.BIGINT, col.getType().getPrimitiveType());
                        break;
                    case "k3":
                        Assert.assertEquals(PrimitiveType.FLOAT, col.getType().getPrimitiveType());
                        break;
                    case "k4":
                        Assert.assertEquals(PrimitiveType.DOUBLE, col.getType().getPrimitiveType());
                        break;
                    case "k5":
                        Assert.assertEquals(PrimitiveType.SMALLINT, col.getType().getPrimitiveType());
                        break;
                    case "k6":
                        Assert.assertEquals(PrimitiveType.TINYINT, col.getType().getPrimitiveType());
                        break;
                    case "k7":
                        Assert.assertEquals(PrimitiveType.BOOLEAN, col.getType().getPrimitiveType());
                        break;
                    case "k8":
                        Assert.assertEquals(PrimitiveType.STRING, col.getType().getPrimitiveType());
                        break;
                    case "k9":
                        Assert.assertEquals(PrimitiveType.DATEV2, col.getType().getPrimitiveType());
                        break;
                    case "k10":
                        Assert.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    case "k11":
                        Assert.assertEquals(PrimitiveType.DECIMAL64, col.getType().getPrimitiveType());
                        break;
                    case "k12":
                        Assert.assertEquals(PrimitiveType.DECIMAL128, col.getType().getPrimitiveType());
                        break;
                    case "k13":
                        Assert.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    default:
                        Assert.fail("unknown column name: " + col.getName());
                }
            }
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testS3ExpressMarkerIsScopedToOneShotInsertStatement() throws AnalysisException {
        boolean previousRunningUnitTest = FeConstants.runningUnitTest;
        ConnectContext previousContext = ConnectContext.get();
        FeConstants.runningUnitTest = true;
        try {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("uri", "s3://analytics--usw2-az1--x-s3/data/file.csv");
            properties.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
            properties.put("s3.region", "us-west-2");
            properties.put("format", "csv");

            S3TableValuedFunction directTvf = new S3TableValuedFunction(properties);
            Assert.assertFalse(directTvf.getBackendConnectProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
            Assert.assertFalse(directTvf.getBrokerDesc().getBackendConfigProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));

            ConnectContext context = new ConnectContext();
            StatementContext statementContext = new StatementContext(
                    context, new OriginStatement("insert into t select * from s3(...)", 0));
            context.setStatementContext(statementContext);
            context.setThreadLocalInfo();

            S3TableValuedFunction queryTvf = (S3TableValuedFunction) new S3(new Properties(properties))
                    .getCatalogFunction();
            Assert.assertFalse(queryTvf.getBackendConnectProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));

            statementContext.setS3ExpressImportRead(true);
            S3TableValuedFunction insertTvf = (S3TableValuedFunction) new S3(new Properties(properties))
                    .getCatalogFunction();
            Assert.assertEquals("true", insertTvf.getBackendConnectProperties()
                    .get(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
            Assert.assertEquals("true", insertTvf.getBrokerDesc().getBackendConfigProperties()
                    .get(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
            Assert.assertFalse(insertTvf.processedParams
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));

            statementContext.setS3ExpressImportRead(false);
            S3TableValuedFunction streamingTvf = (S3TableValuedFunction) new S3(new Properties(properties))
                    .getCatalogFunction();
            Assert.assertFalse(streamingTvf.getBackendConnectProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
        } finally {
            FeConstants.runningUnitTest = previousRunningUnitTest;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
