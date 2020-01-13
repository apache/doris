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

package org.apache.doris.common.util;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.KuduPartition.KuduRange;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

public class KuduUtil {

    public static final int MAX_COLUMN_NUM = 300;

    // Number of worker threads created by each KuduClient, regardless of whether or not
    // they're needed. Impala does not share KuduClients between operations, so the number
    // of threads created can get very large under concurrent workloads. This number should
    // be sufficient for the Frontend/Catalog use, and has been tested in stress tests.
    private static int KUDU_CLIENT_WORKER_THREAD_COUNT = 5;

    /**
     * Creates a KuduClient with the specified Kudu master addresses (as a comma-separated list of host:port pairs). The
     * 'admin operation timeout' and the 'operation timeout' are set to BackendConfig.getKuduClientTimeoutMs(). The
     * 'admin operations timeout' is used for operations like creating/deleting tables. The 'operation timeout' is used
     * when fetching tablet metadata.
     */
    public static KuduClient createKuduClient(String kuduMasters) {
        KuduClientBuilder b = new KuduClient.KuduClientBuilder(kuduMasters);
        b.defaultAdminOperationTimeoutMs(Config.kudu_client_timeout_ms);
        b.defaultOperationTimeoutMs(Config.kudu_client_timeout_ms);
        b.workerCount(KUDU_CLIENT_WORKER_THREAD_COUNT);
        return b.build();
    }

    public static boolean isSupportedKeyType(PrimitiveType type) {
        return type.isIntegerType() || type.isStringType();
    }

    /*
     * add each range partition meta data to CreateTableOptions from RangePartitionDesc
     */
    public static void setRangePartitionInfo(Schema kuduSchema,
                                             RangePartitionDesc rangePart,
                                             CreateTableOptions kuduCreateOpts,
                                             List<KuduRange> kuduRanges)
            throws DdlException {

        List<String> partColNames = rangePart.getPartitionColNames();
        Preconditions.checkState(partColNames.size() == 1);
        String partColName = partColNames.get(0);
        kuduCreateOpts.setRangePartitionColumns(partColNames);

        ColumnSchema kuduColumnSchema = kuduSchema.getColumn(partColName);
        Preconditions.checkNotNull(kuduColumnSchema);
        Type kuduType = kuduColumnSchema.getType();

        boolean first = true;
        SingleRangePartitionDesc lastDesc = null;
        for (SingleRangePartitionDesc single : rangePart.getSingleRangePartitionDescs()) {
            // 1. create kudu range
            PartialRow lower = kuduSchema.newPartialRow();
            PartialRow upper = kuduSchema.newPartialRow();

            String lowerValue = lastDesc != null ? lastDesc.getPartitionKeyDesc().getUpperValues().get(0).getStringValue() : "";
            String upperValue = single.getPartitionKeyDesc().getUpperValues().get(0).getStringValue();

            switch (kuduType) {
                case INT8:
                    if (first) {
                        lower.addByte(partColName, Byte.MIN_VALUE);
                        first = false;
                    } else {
                        Preconditions.checkNotNull(lastDesc);
                        lower.addByte(partColName, Byte.valueOf(lowerValue));
                    }
                    upper.addByte(partColName, Byte.valueOf(upperValue));
                    break;
                case INT16:
                    if (first) {
                        lower.addShort(partColName, Short.MIN_VALUE);
                        first = false;
                    } else {
                        Preconditions.checkNotNull(lastDesc);
                        lower.addShort(partColName, Short.valueOf(lowerValue));
                    }
                    upper.addShort(partColName, Short.valueOf(upperValue));
                    break;
                case INT32:
                    if (first) {
                        lower.addInt(partColName, Integer.MIN_VALUE);
                        first = false;
                    } else {
                        Preconditions.checkNotNull(lastDesc);
                        lower.addInt(partColName, Integer.valueOf(lowerValue));
                    }
                    upper.addInt(partColName, Integer.valueOf(upperValue));
                    break;
                case INT64:
                    if (first) {
                        lower.addLong(partColName, Long.MIN_VALUE);
                        first = false;
                    } else {
                        Preconditions.checkNotNull(lastDesc);
                        lower.addLong(partColName, Long.valueOf(lowerValue));
                    }
                    upper.addLong(partColName, Long.valueOf(upperValue));
                    break;
                case STRING:
                    if (first) {
                        lower.addStringUtf8(partColName, "".getBytes(Charset.forName("Utf-8")));
                        first = false;
                    } else {
                        Preconditions.checkNotNull(lastDesc);
                        lower.addStringUtf8(partColName, lowerValue.getBytes(Charset.forName("Utf-8")));
                    }
                    upper.addStringUtf8(partColName, upperValue.getBytes(Charset.forName("Utf-8")));
                    break;
                default:
                    throw new DdlException("Kudu does not support col type [" + kuduType + "] for partition");
            }
            kuduCreateOpts.addRangePartition(lower, upper,
                                             RangePartitionBound.INCLUSIVE_BOUND,
                                             RangePartitionBound.EXCLUSIVE_BOUND);

            // 2. create mapping range
            KuduRange kuduRange = new KuduRange(single.getPartitionName(), lower, upper,
                                                RangePartitionBound.INCLUSIVE_BOUND,
                                                RangePartitionBound.EXCLUSIVE_BOUND);
            kuduRanges.add(kuduRange);

            lastDesc = single;
        }
    }

    /*
     * Create kudu Column Schemas from Catalog Column
     */
    public static List<ColumnSchema> generateKuduColumn(List<Column> columns) throws DdlException {
        List<ColumnSchema> kuduColumns = Lists.newArrayList();
        for (Column column : columns) {
            org.apache.kudu.Type kuduType = toKuduType(column.getType().getPrimitiveType());
            
            Object defaultValue = getKuduDefaultValue(column);
            ColumnSchema columnSchema =
                    new ColumnSchema.ColumnSchemaBuilder(column.getName(), kuduType).key(column.isKey())
                            .nullable(column.isAllowNull()).defaultValue(defaultValue).build();
            kuduColumns.add(columnSchema);
        }

        return kuduColumns;
    }

    private static Object getKuduDefaultValue(Column column) {
        if (column.getDefaultValue() == null) {
            return null;
        }
        Object defaultValue = null;
        switch (column.getDataType()) {
            case TINYINT:
                defaultValue = Byte.valueOf(column.getDefaultValue());
                break;
            case SMALLINT:
                defaultValue = Short.valueOf(column.getDefaultValue());
                break;
            case INT:
                defaultValue = Integer.valueOf(column.getDefaultValue());
                break;
            case BIGINT:
                defaultValue = Long.valueOf(column.getDefaultValue());
                break;
            case FLOAT:
                defaultValue = Float.valueOf(column.getDefaultValue());
                break;
            case DOUBLE:
                defaultValue = Double.valueOf(column.getDefaultValue());
                break;
            case CHAR:
                defaultValue = column.getDefaultValue();
                break;
            default:
                Preconditions.checkState(false);
        }
        return defaultValue;
    }

    public static Type toKuduType(PrimitiveType primitiveType) throws DdlException {
        Type kuduType = null;
        switch (primitiveType) {
            case TINYINT:
                kuduType = Type.INT8;
                break;
            case SMALLINT:
                kuduType = Type.INT16;
                break;
            case INT:
                kuduType = Type.INT32;
                break;
            case BIGINT:
                kuduType = Type.INT64;
                break;
            case FLOAT:
                kuduType = Type.FLOAT;
                break;
            case DOUBLE:
                kuduType = Type.DOUBLE;
                break;
            case CHAR:
                kuduType = Type.STRING;
                break;
            default:
                ErrorReport.reportDdlException(ErrorCode.ERR_KUDU_NOT_SUPPORT_VALUE_TYPE,
                                               primitiveType);
        }
        return kuduType;
    }

    public static void analyzeColumn(Column col, KeysDesc keysDesc) throws AnalysisException {
        if (col.getName() == null || col.getDataType() == null) {
            throw new AnalysisException("No column name or column type in column definition.");
        }

        FeNameFormat.checkColumnName(col.getName());

        if (col.getType().isComplexType()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_KUDU_NOT_SUPPORT_COMPLEX_VALUE);
        }

        switch (col.getDataType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case BINARY:
                break;
            default:
                throw new AnalysisException("Kudu table does not support column type: "
                        + col.getDataType());
        }

        if (col.getAggregationType() != null) {
            throw new AnalysisException("Kudu table does not support Aggr columns");
        }

        if (col.getDefaultValue() != null) {
            ColumnDef.validateDefaultValue(col.getOriginType(), col.getDefaultValue());
        }

        if (keysDesc.containsCol(col.getName())) {
            if (col.isAllowNull()) {
                throw new AnalysisException("Primary key in kudu table does not allow nullable");
            }
            col.setIsKey(true);
        } else {
            col.setIsKey(false);
        }
    }

    public static void analyzeKeyDesc(KeysDesc keysDesc) throws AnalysisException {
        if (keysDesc.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new AnalysisException("Create kudu table should specify PRIMARY KEYS");
        }
    }

    public static void analyzePartitionAndDistributionDesc(KeysDesc keysDesc,
                                                           PartitionDesc partitionDesc,
                                                           DistributionDesc distributionDesc)
            throws AnalysisException {
        if (partitionDesc == null && distributionDesc == null) {
            throw new AnalysisException("Kudu table should be specified at lease one of partition method");
        }

        if (partitionDesc != null) {
            if (!(partitionDesc instanceof RangePartitionDesc)) {
                throw new AnalysisException("Kudu table only permit range partition");
            }

            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            analyzePartitionDesc(keysDesc, rangePartitionDesc);
        }

        if (distributionDesc != null) {
            if (!(distributionDesc instanceof HashDistributionDesc)) {
                throw new AnalysisException("Kudu table only permit hash distribution");
            }

            HashDistributionDesc hashDistributionDesc = (HashDistributionDesc) distributionDesc;
            analyzeDistributionDesc(keysDesc, hashDistributionDesc);
        }
    }

    private static void analyzePartitionDesc(KeysDesc keysDesc, RangePartitionDesc partDesc)
            throws AnalysisException {
        if (partDesc.getPartitionColNames() == null || partDesc.getPartitionColNames().isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        for (String partColName : partDesc.getPartitionColNames()) {
            if (!keysDesc.containsCol(partColName)) {
                throw new AnalysisException("Kudu table's parition column must be the subset of primary keys");
            }
        }

        Set<String> nameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SingleRangePartitionDesc desc : partDesc.getSingleRangePartitionDescs()) {
            if (nameSet.contains(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            desc.analyze(partDesc.getPartitionColNames().size(), null);
            nameSet.add(desc.getPartitionName());
        }
    }

    private static void analyzeDistributionDesc(KeysDesc keysDesc, HashDistributionDesc distDesc)
            throws AnalysisException {
        if (distDesc.getBuckets() <= 1) {
            throw new AnalysisException("Kudu table's hash distribution buckets num should > 1.");
        }

        if (distDesc.getDistributionColumnNames() == null || distDesc.getDistributionColumnNames().size() == 0) {
            throw new AnalysisException("Number of hash column is zero.");
        }

        for (String columnName : distDesc.getDistributionColumnNames()) {
            if (!keysDesc.containsCol(columnName)) {
                throw new AnalysisException("Kudu table's distribution column must be the subset of primary keys");
            }
        }
    }
}
