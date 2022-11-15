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

package org.apache.doris.statistics;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class StatisticsUtil {

    public static List<ResultRow> execStatisticQuery(String sql) {
        ConnectContext connectContext = StatisticsUtil.buildConnectContext();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setExecutor(stmtExecutor);
        return stmtExecutor.executeInternalQuery();
    }

    public static void execUpdate(String sql) throws Exception {
        ConnectContext connectContext = StatisticsUtil.buildConnectContext();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setExecutor(stmtExecutor);
        stmtExecutor.execute();
    }

    // TODO: finish this.
    public static List<AnalysisJobInfo> deserializeToAnalysisJob(List<ResultRow> resultBatches) throws TException {
        return new ArrayList<>();
    }

    public static List<ColumnStatistic> deserializeToColumnStatistics(List<ResultRow> resultBatches)
            throws Exception {
        return resultBatches.stream().map(ColumnStatistic::fromResultRow).collect(Collectors.toList());
    }

    public static ConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setMaxExecMemByte(Config.statistics_max_mem_per_query_in_bytes);
        sessionVariable.setEnableInsertStrict(true);
        sessionVariable.parallelExecInstanceNum = Config.statistic_parallel_exec_instance_num;
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(StatisticConstants.STATISTIC_DB_NAME);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setThreadLocalInfo();
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        return connectContext;
    }

    public static void analyze(StatementBase statementBase) throws UserException {
        Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), buildConnectContext());
        statementBase.analyze(analyzer);
    }

    public static LiteralExpr readableValue(Type type, String columnValue) throws AnalysisException {
        Preconditions.checkArgument(type.isScalarType());
        ScalarType scalarType = (ScalarType) type;

        // check if default value is valid.
        // if not, some literal constructor will throw AnalysisException
        PrimitiveType primitiveType = scalarType.getPrimitiveType();
        switch (primitiveType) {
            case BOOLEAN:
                return new BoolLiteral(columnValue);
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new IntLiteral(columnValue, type);
            case LARGEINT:
                return new LargeIntLiteral(columnValue);
            case FLOAT:
                // the min max value will loose precision when value type is double.
            case DOUBLE:
                return new FloatLiteral(columnValue);
            case DECIMALV2:
                //no need to check precision and scale, since V2 is fixed point
                return new DecimalLiteral(columnValue);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                DecimalLiteral decimalLiteral = new DecimalLiteral(columnValue);
                decimalLiteral.checkPrecisionAndScale(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                return decimalLiteral;
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                return new DateLiteral(columnValue, type);
            case CHAR:
            case VARCHAR:
                if (columnValue.length() > scalarType.getLength()) {
                    throw new AnalysisException("Min/Max value is longer than length of column type: "
                        + columnValue);
                }
                return new StringLiteral(columnValue);
            case HLL:
            case BITMAP:
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                throw new AnalysisException("Unsupported setting this type: " + type + " of min max value");
        }
    }

    public static double convertToDouble(Type type, String columnValue) throws AnalysisException {
        Preconditions.checkArgument(type.isScalarType());
        try {
            ScalarType scalarType = (ScalarType) type;

            // check if default value is valid.
            // if not, some literal constructor will throw AnalysisException
            PrimitiveType primitiveType = scalarType.getPrimitiveType();
            switch (primitiveType) {
                case BOOLEAN:
                    return Boolean.parseBoolean(columnValue) ? 1.0 : 0.0;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                case FLOAT:
                    // the min max value will loose precision when value type is double.
                case DOUBLE:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return Double.parseDouble(columnValue);
                case DATE:
                case DATEV2:
                    org.apache.doris.nereids.trees.expressions.literal.DateLiteral literal =
                            new org.apache.doris.nereids.trees.expressions.literal.DateLiteral(columnValue);
                    return literal.getDouble();

                case DATETIMEV2:
                case DATETIME:
                    DateTimeLiteral dateTimeLiteral = new DateTimeLiteral(columnValue);
                    return dateTimeLiteral.getDouble();
                case CHAR:
                case VARCHAR:
                    return convertStringToDouble(columnValue);
                case HLL:
                case BITMAP:
                case ARRAY:
                case MAP:
                case STRUCT:
                default:
                    throw new AnalysisException("Unsupported setting this type: " + type + " of min max value");
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }

    }

    public static double convertStringToDouble(String s) {
        long v = 0;
        int pos = 0;
        int len = Math.min(s.length(), 8);
        while (pos < len) {
            v += ((long) s.charAt(pos)) << ((7 - pos) * 8);
            pos++;
        }
        return (double) v;
    }

}
