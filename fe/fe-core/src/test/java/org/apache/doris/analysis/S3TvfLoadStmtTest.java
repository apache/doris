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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.constants.S3Properties.Env;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class S3TvfLoadStmtTest extends TestWithFeService {

    private static final String ACCESS_KEY_VALUE = "ak";

    private static final String SECRET_KEY_VALUE = "sk";

    private static final String ENDPOINT_VALUE = "cos.ap-beijing.myqcloud.com";

    private static final String REGION_VALUE = "ap-beijing";

    private static final String DATA_URI = "s3://doris-build-1308700295/regression/load/data/part*";

    private static final String FORMAT = "parquet";

    private static final String TARGET_TABLE_NAME = "target";

    private LabelName labelName;

    private BrokerDesc brokerDesc;

    private Set<String> colNames;

    @BeforeAll
    public void setUp() throws AnalysisException {
        FeConstants.runningUnitTest = true;

        labelName = new LabelName("testDb", "testTbl");

        final Map<String, String> brokerProperties = Maps.newHashMap();
        brokerProperties.put(Env.ACCESS_KEY, ACCESS_KEY_VALUE);
        brokerProperties.put(Env.SECRET_KEY, SECRET_KEY_VALUE);
        brokerProperties.put(Env.ENDPOINT, ENDPOINT_VALUE);
        brokerProperties.put(Env.REGION, REGION_VALUE);
        brokerDesc = new BrokerDesc("s3", StorageType.S3, brokerProperties);

        colNames = Sets.newHashSet("k1", "k2", "k3", "k4");
    }

    @Test
    public void testClauses() throws UserException {
        final BinaryPredicate greater = new BinaryPredicate(Operator.GT, new IntLiteral(1), new IntLiteral(0));
        final BinaryPredicate less = new BinaryPredicate(Operator.LT, new IntLiteral(1), new IntLiteral(0));
        DataDescription dataDescription = buildDataDesc(
                Lists.newArrayList(colNames),
                greater,
                less,
                null
        );
        final S3TvfLoadStmt s3TvfLoadStmt = new S3TvfLoadStmt(labelName, Collections.singletonList(dataDescription),
                brokerDesc,
                Maps.newHashMap(), "comment");
        final SelectStmt selectStmt = (SelectStmt) s3TvfLoadStmt.getQueryStmt();
        final Expr whereClause = Deencapsulation.getField(selectStmt, "whereClause");
        Assertions.assertEquals(whereClause, new CompoundPredicate(CompoundPredicate.Operator.AND, greater, less));
    }

    @Test
    public void testTvfGeneration() {
        DataDescription dataDescription = buildDataDesc(
                Lists.newArrayList(colNames),
                null,
                null,
                null
        );
        final TableRef tvfRef = Deencapsulation.invoke(S3TvfLoadStmt.class,
                "buildTvfRef",
                dataDescription, brokerDesc);
        Assertions.assertTrue(tvfRef instanceof TableValuedFunctionRef);
        final S3TableValuedFunction tableFunction
                = (S3TableValuedFunction) ((TableValuedFunctionRef) tvfRef).getTableFunction();
        final Map<String, String> locationProperties = tableFunction.getLocationProperties();
        Assertions.assertEquals(locationProperties.get(S3Properties.ENDPOINT), ENDPOINT_VALUE);
        Assertions.assertEquals(locationProperties.get(S3Properties.ACCESS_KEY), ACCESS_KEY_VALUE);
        Assertions.assertEquals(locationProperties.get(S3Properties.SECRET_KEY), SECRET_KEY_VALUE);
        Assertions.assertEquals(locationProperties.get(S3Properties.REGION), REGION_VALUE);
        Assertions.assertEquals(tableFunction.getFilePath(), DATA_URI);
    }

    @Injectable
    Table targetTable;

    @Test
    public void testColumnMappings() throws Exception {
        // c1/c2/c3 in both file and table, and c5 is only in table
        final List<ImportColumnDesc> columnsDescList = getColumnsDescList(
                "c1,c2,c3,c1=upper(c1), tmp_c4=c1 + 1, c5 = tmp_c4+1");
        DataDescription dataDescription = buildDataDesc(colNames, null, null, null);
        new Expectations() {
            {
                dataDescription.getParsedColumnExprList();
                minTimes = 0;
                result = columnsDescList;

                dataDescription.getFilePaths();
                minTimes = 0;
                result = Collections.singletonList(DATA_URI);

                targetTable.getBaseSchema();
                minTimes = 0;
                result = getBaseSchema();

                targetTable.getColumn("c1");
                minTimes = 0;
                result = new Column();

                targetTable.getColumn("c2");
                minTimes = 0;
                result = new Column();

                targetTable.getColumn("c3");
                minTimes = 0;
                result = new Column();

                targetTable.getColumn("c5");
                minTimes = 0;
                result = new Column();

                targetTable.getColumn("tmp_c4");
                minTimes = 0;
                result = null;
            }
        };
        final S3TvfLoadStmt s3TvfLoadStmt = new S3TvfLoadStmt(labelName, Collections.singletonList(dataDescription),
                brokerDesc, null, "comment");
        s3TvfLoadStmt.setTargetTable(targetTable);
        Deencapsulation.setField(s3TvfLoadStmt, "functionGenTableColNames", Sets.newHashSet("c1", "c2", "c3"));

        Deencapsulation.invoke(s3TvfLoadStmt, "rewriteExpr", columnsDescList);
        Assertions.assertEquals(columnsDescList.size(), 5);
        final String orig4 = "((upper(`c1`) + 1) + 1)";
        Assertions.assertEquals(orig4, columnsDescList.get(4).getExpr().toString());

        final List<ImportColumnDesc> filterColumns = Deencapsulation.invoke(s3TvfLoadStmt,
                "filterColumns", columnsDescList);
        Assertions.assertEquals(filterColumns.size(), 4);
    }

    private static DataDescription buildDataDesc(Iterable<String> columns, Expr fileFilter, Expr wherePredicate,
            List<Expr> mappingList) {

        return new DataDescription(
                TARGET_TABLE_NAME,
                null,
                Collections.singletonList(DATA_URI),
                Lists.newArrayList(columns),
                null,
                FORMAT,
                null,
                false,
                mappingList,
                fileFilter,
                wherePredicate,
                MergeType.APPEND,
                null,
                null,
                null
        );
    }

    private static List<ImportColumnDesc> getColumnsDescList(String columns) throws Exception {
        String columnsSQL = "COLUMNS (" + columns + ")";
        return ((ImportColumnsStmt) SqlParserUtils.getFirstStmt(
                new org.apache.doris.analysis.SqlParser(
                        new org.apache.doris.analysis.SqlScanner(new StringReader(columnsSQL))))).getColumns();
    }

    private static List<Column> getBaseSchema() {
        List<Column> columns = Lists.newArrayList();

        Column c1 = new Column("c1", PrimitiveType.BIGINT);
        c1.setIsKey(true);
        c1.setIsAllowNull(false);
        columns.add(c1);

        Column c2 = new Column("c2", ScalarType.createVarchar(25));
        c2.setIsKey(true);
        c2.setIsAllowNull(true);
        columns.add(c2);

        Column c3 = new Column("c3", PrimitiveType.BIGINT);
        c3.setIsKey(true);
        c3.setIsAllowNull(false);
        columns.add(c3);

        Column c5 = new Column("c5", PrimitiveType.BIGINT);
        c5.setIsKey(true);
        c5.setIsAllowNull(true);
        columns.add(c5);

        return columns;
    }

}
