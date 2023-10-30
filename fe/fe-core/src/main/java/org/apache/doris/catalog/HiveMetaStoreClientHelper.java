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

package org.apache.doris.catalog;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.backup.BlobStorage;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExprOpcode;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper class for HiveMetaStoreClient
 */
public class HiveMetaStoreClientHelper {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreClientHelper.class);

    public static final String COMMENT = "comment";

    private static final Pattern digitPattern = Pattern.compile("(\\d+)");

    public enum HiveFileFormat {
        TEXT_FILE(0, "text"),
        PARQUET(1, "parquet"),
        ORC(2, "orc");

        private int index;
        private String desc;

        HiveFileFormat(int index, String desc) {
            this.index = index;
            this.desc = desc;
        }

        public int getIndex() {
            return index;
        }

        public String getDesc() {
            return desc;
        }

        /**
         * convert Hive table inputFormat to file format
         * @param input inputFormat of Hive file
         * @return
         * @throws DdlException
         */
        public static String getFormat(String input) throws DdlException {
            String formatDesc = "";
            for (HiveFileFormat format : HiveFileFormat.values()) {
                String lowerCaseInput = input.toLowerCase();
                if (lowerCaseInput.contains(format.getDesc())) {
                    formatDesc = format.getDesc();
                    break;
                }
            }
            if (Strings.isNullOrEmpty(formatDesc)) {
                LOG.warn("Not supported Hive file format [{}].", input);
                throw new DdlException("Not supported Hive file format " + input);
            }
            return formatDesc;
        }
    }

    public static IMetaStoreClient getClient(String metaStoreUris) throws DdlException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUris);
        hiveConf.set(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                String.valueOf(Config.hive_metastore_client_timeout_second));
        IMetaStoreClient metaStoreClient = null;
        String type = hiveConf.get(HMSResource.HIVE_METASTORE_TYPE);
        try {
            if ("dlf".equalsIgnoreCase(type)) {
                // For aliyun DLF
                metaStoreClient = new ProxyMetaStoreClient(hiveConf);
            } else {
                metaStoreClient = new HiveMetaStoreClient(hiveConf);
            }
        } catch (MetaException e) {
            LOG.warn("Create HiveMetaStoreClient failed: {}", e.getMessage());
            throw new DdlException("Create HiveMetaStoreClient failed: " + e.getMessage());
        }
        return metaStoreClient;
    }

    /**
     * Get data files of partitions in hive table, filter by partition predicate.
     *
     * @param hiveTable
     * @param hivePartitionPredicate
     * @param fileStatuses
     * @param remoteHiveTbl
     * @return
     * @throws DdlException
     */
    public static String getHiveDataFiles(HiveTable hiveTable, ExprNodeGenericFuncDesc hivePartitionPredicate,
            List<TBrokerFileStatus> fileStatuses, Table remoteHiveTbl, StorageBackend.StorageType type)
            throws DdlException {
        BlobStorage storage = BlobStorage.create("HiveMetaStore", type, hiveTable.getHiveProperties());
        List<RemoteIterator<LocatedFileStatus>> remoteIterators = new ArrayList<>();
        try {
            if (remoteHiveTbl.getPartitionKeys().size() > 0) {
                String metaStoreUris = hiveTable.getHiveProperties().get(HMSResource.HIVE_METASTORE_URIS);
                // hive partitioned table, get file iterator from table partition sd info
                List<Partition> hivePartitions = getHivePartitions(metaStoreUris, remoteHiveTbl,
                        hivePartitionPredicate);
                for (Partition p : hivePartitions) {
                    String location = normalizeS3LikeSchema(p.getSd().getLocation());
                    remoteIterators.add(storage.listLocatedStatus(location));
                }
            } else {
                // hive non-partitioned table, get file iterator from table sd info
                String location = normalizeS3LikeSchema(remoteHiveTbl.getSd().getLocation());
                remoteIterators.add(storage.listLocatedStatus(location));
            }
            return getAllFileStatus(fileStatuses, remoteIterators, storage);
        } catch (UserException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    public static String normalizeS3LikeSchema(String location) {
        String[] objectStorages = Config.s3_compatible_object_storages.split(",");
        for (String objectStorage : objectStorages) {
            if (location.startsWith(objectStorage + "://")) {
                location = location.replaceFirst(objectStorage, "s3");
                break;
            }
        }
        return location;
    }

    private static String getAllFileStatus(List<TBrokerFileStatus> fileStatuses,
            List<RemoteIterator<LocatedFileStatus>> remoteIterators, BlobStorage storage) throws UserException {
        boolean needFullPath = storage.getStorageType() == StorageBackend.StorageType.S3
                || storage.getStorageType() == StorageBackend.StorageType.OFS
                || storage.getStorageType() == StorageBackend.StorageType.JFS;
        String hdfsUrl = "";
        Queue<RemoteIterator<LocatedFileStatus>> queue = Queues.newArrayDeque(remoteIterators);
        while (queue.peek() != null) {
            RemoteIterator<LocatedFileStatus> iterator = queue.poll();
            try {
                while (iterator.hasNext()) {
                    LocatedFileStatus fileStatus = iterator.next();
                    if (fileStatus.isDirectory()) {
                        // recursive visit the directory to get the file path.
                        queue.add(storage.listLocatedStatus(fileStatus.getPath().toString()));
                        continue;
                    }
                    TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus();
                    brokerFileStatus.setIsDir(fileStatus.isDirectory());
                    brokerFileStatus.setIsSplitable(true);
                    brokerFileStatus.setSize(fileStatus.getLen());
                    // path = "/path/to/partition/file_name"
                    // eg: /home/work/dev/hive/apache-hive-2.3.7-bin/data/warehouse
                    //     + /dae.db/customer/state=CA/city=SanJose/000000_0
                    String path = fileStatus.getPath().toUri().getPath();
                    if (needFullPath) {
                        // Backend need full s3 path (with s3://bucket at the beginning) to read the data on s3.
                        // path = "s3://bucket/path/to/partition/file_name"
                        // eg: s3://hive-s3-test/region/region.tbl
                        path = fileStatus.getPath().toString();
                    }
                    brokerFileStatus.setPath(path);
                    fileStatuses.add(brokerFileStatus);
                    if (StringUtils.isEmpty(hdfsUrl)) {
                        // hdfs://host:port/path/to/partition/file_name
                        String fullUri = fileStatus.getPath().toString();
                        // hdfs://host:port
                        hdfsUrl = fullUri.replace(path, "");
                    }
                }
            } catch (IOException e) {
                LOG.warn("List HDFS file IOException: {}", e.getMessage());
                throw new DdlException("List HDFS file failed. Error: " + e.getMessage());
            }
        }
        return hdfsUrl;
    }

    /**
     * list partitions from hiveMetaStore.
     *
     * @param metaStoreUris hiveMetaStore uris
     * @param remoteHiveTbl Hive table
     * @param hivePartitionPredicate filter when list partitions
     * @return a list of hive partitions
     * @throws DdlException when connect hiveMetaStore failed.
     */
    public static List<Partition> getHivePartitions(String metaStoreUris, Table remoteHiveTbl,
                       ExprNodeGenericFuncDesc hivePartitionPredicate) throws DdlException {
        List<Partition> hivePartitions = new ArrayList<>();
        IMetaStoreClient client = getClient(metaStoreUris);
        try {
            client.listPartitionsByExpr(remoteHiveTbl.getDbName(), remoteHiveTbl.getTableName(),
                    SerializationUtilities.serializeExpressionToKryo(hivePartitionPredicate),
                    null, (short) -1, hivePartitions);
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed: " + e.getMessage());
        } finally {
            client.close();
        }
        return hivePartitions;
    }

    public static Table getTable(HiveTable hiveTable) throws DdlException {
        IMetaStoreClient client = getClient(hiveTable.getHiveProperties().get(HMSResource.HIVE_METASTORE_URIS));
        Table table;
        try {
            table = client.getTable(hiveTable.getHiveDb(), hiveTable.getHiveTable());
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed. Error: " + e.getMessage());
        }
        return table;
    }

    /**
     * Get hive table with dbName and tableName.
     * Only for Hudi.
     *
     * @param dbName database name
     * @param tableName table name
     * @param metaStoreUris hive metastore uris
     * @return HiveTable
     * @throws DdlException when get table from hive metastore failed.
     */
    @Deprecated
    public static Table getTable(String dbName, String tableName, String metaStoreUris) throws DdlException {
        IMetaStoreClient client = getClient(metaStoreUris);
        Table table;
        try {
            table = client.getTable(dbName, tableName);
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed. Error: " + e.getMessage());
        } finally {
            client.close();
        }
        return table;
    }

    /**
     * Convert Doris expr to Hive expr, only for partition column
     * @param tblName
     * @return
     * @throws DdlException
     * @throws SemanticException
     */
    public static ExprNodeGenericFuncDesc convertToHivePartitionExpr(List<Expr> conjuncts,
            List<String> partitionKeys, String tblName) throws DdlException {
        List<ExprNodeDesc> hivePredicates = new ArrayList<>();

        for (Expr conjunct : conjuncts) {
            ExprNodeGenericFuncDesc hiveExpr = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    conjunct, partitionKeys, tblName).getFuncDesc();
            if (hiveExpr != null) {
                hivePredicates.add(hiveExpr);
            }
        }
        int count = hivePredicates.size();
        // combine all predicate by `and`
        // compoundExprs must have at least 2 predicates
        if (count >= 2) {
            return HiveMetaStoreClientHelper.getCompoundExpr(hivePredicates, "and");
        } else if (count == 1) {
            // only one predicate
            return (ExprNodeGenericFuncDesc) hivePredicates.get(0);
        } else {
            return genAlwaysTrueExpr(tblName);
        }
    }

    private static ExprNodeGenericFuncDesc genAlwaysTrueExpr(String tblName) throws DdlException {
        // have no predicate, make a dummy predicate "1=1" to get all partitions
        HiveMetaStoreClientHelper.ExprBuilder exprBuilder =
                new HiveMetaStoreClientHelper.ExprBuilder(tblName);
        return exprBuilder.val(TypeInfoFactory.intTypeInfo, 1)
                .val(TypeInfoFactory.intTypeInfo, 1)
                .pred("=", 2).build();
    }

    private static class ExprNodeGenericFuncDescContext {
        private static final ExprNodeGenericFuncDescContext BAD_CONTEXT = new ExprNodeGenericFuncDescContext();

        private ExprNodeGenericFuncDesc funcDesc = null;
        private boolean eligible = false;

        public ExprNodeGenericFuncDescContext(ExprNodeGenericFuncDesc funcDesc) {
            this.funcDesc = funcDesc;
            this.eligible = true;
        }

        private ExprNodeGenericFuncDescContext() {
        }

        /**
         * Check eligible before use the expr in CompoundPredicate for `and` and `or` .
         */
        public boolean isEligible() {
            return eligible;
        }

        public ExprNodeGenericFuncDesc getFuncDesc() {
            return funcDesc;
        }
    }

    private static ExprNodeGenericFuncDescContext convertToHivePartitionExpr(Expr dorisExpr,
            List<String> partitionKeys, String tblName) throws DdlException {
        if (dorisExpr == null) {
            return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
        }

        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            ExprNodeGenericFuncDescContext left = convertToHivePartitionExpr(
                    compoundPredicate.getChild(0), partitionKeys, tblName);
            ExprNodeGenericFuncDescContext right = convertToHivePartitionExpr(
                    compoundPredicate.getChild(1), partitionKeys, tblName);

            switch (compoundPredicate.getOp()) {
                case AND: {
                    if (left.isEligible() && right.isEligible()) {
                        List<ExprNodeDesc> andArgs = new ArrayList<>();
                        andArgs.add(left.getFuncDesc());
                        andArgs.add(right.getFuncDesc());
                        return new ExprNodeGenericFuncDescContext(getCompoundExpr(andArgs, "and"));
                    } else if (left.isEligible()) {
                        return left;
                    } else if (right.isEligible()) {
                        return right;
                    } else {
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                case OR: {
                    if (left.isEligible() && right.isEligible()) {
                        List<ExprNodeDesc> andArgs = new ArrayList<>();
                        andArgs.add(left.getFuncDesc());
                        andArgs.add(right.getFuncDesc());
                        return new ExprNodeGenericFuncDescContext(getCompoundExpr(andArgs, "or"));
                    } else {
                        // If it is not a partition key, this is an always true expr.
                        // Or if is a partition key and also is a not supportedOp, this is an always true expr.
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                default:
                    // TODO: support NOT predicate for CompoundPredicate
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
            }
        }
        return binaryExprDesc(dorisExpr, partitionKeys, tblName);
    }

    private static ExprNodeGenericFuncDescContext binaryExprDesc(Expr dorisExpr,
            List<String> partitionKeys, String tblName) throws DdlException {
        TExprOpcode opcode = dorisExpr.getOpcode();
        switch (opcode) {
            case EQ:
            case NE:
            case GE:
            case GT:
            case LE:
            case LT:
            case EQ_FOR_NULL:
                BinaryPredicate eq = (BinaryPredicate) dorisExpr;
                // Make sure the col slot is always first
                SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                LiteralExpr literalExpr = convertDorisExprToLiteralExpr(eq.getChild(1));
                if (slotRef == null || literalExpr == null) {
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
                String colName = slotRef.getColumnName();
                // check whether colName is partition column or not
                if (!partitionKeys.contains(colName)) {
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
                PrimitiveType dorisPrimitiveType = slotRef.getType().getPrimitiveType();
                PrimitiveTypeInfo hivePrimitiveType = convertToHiveColType(dorisPrimitiveType);
                Object value = extractDorisLiteral(literalExpr);
                if (value == null) {
                    if (opcode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                        return genExprDesc(tblName, hivePrimitiveType, colName,  "NULL", "=");
                    } else {
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                switch (opcode) {
                    case EQ:
                    case EQ_FOR_NULL:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, "=");
                    case NE:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, "!=");
                    case GE:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, ">=");
                    case GT:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, ">");
                    case LE:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, "<=");
                    case LT:
                        return genExprDesc(tblName, hivePrimitiveType, colName,  value, "<");
                    default:
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
            default:
                // TODO: support in predicate
                return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
        }
    }

    private static ExprNodeGenericFuncDescContext genExprDesc(
            String tblName,
            PrimitiveTypeInfo hivePrimitiveType,
            String colName,
            Object value,
            String op) throws DdlException {
        ExprBuilder exprBuilder = new ExprBuilder(tblName);
        exprBuilder.col(hivePrimitiveType, colName).val(hivePrimitiveType, value);
        return new ExprNodeGenericFuncDescContext(exprBuilder.pred(op, 2).build());
    }

    public static ExprNodeGenericFuncDesc getCompoundExpr(List<ExprNodeDesc> args, String op) throws DdlException {
        ExprNodeGenericFuncDesc compoundExpr;
        try {
            compoundExpr = ExprNodeGenericFuncDesc.newInstance(
                    FunctionRegistry.getFunctionInfo(op).getGenericUDF(), args);
        } catch (SemanticException e) {
            LOG.warn("Convert to Hive expr failed: {}", e.getMessage());
            throw new DdlException("Convert to Hive expr failed. Error: " + e.getMessage());
        }
        return compoundExpr;
    }

    public static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    public static LiteralExpr convertDorisExprToLiteralExpr(Expr expr) {
        LiteralExpr literalExpr = null;
        if (expr instanceof LiteralExpr) {
            literalExpr = (LiteralExpr) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof LiteralExpr) {
                literalExpr = (LiteralExpr) expr.getChild(0);
            }
        }
        return literalExpr;
    }

    public static Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
            StringBuilder sb = new StringBuilder();
            sb.append(dateLiteral.getYear())
                    .append(dateLiteral.getMonth())
                    .append(dateLiteral.getDay())
                    .append(dateLiteral.getHour())
                    .append(dateLiteral.getMinute())
                    .append(dateLiteral.getSecond());
            Date date;
            try {
                date = formatter.parse(sb.toString());
            } catch (ParseException e) {
                return null;
            }
            return date.getTime();
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            return decimalLiteral.getValue();
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return stringLiteral.getStringValue();
        }
        return null;
    }

    /**
     * Convert from Doris column type to Hive column type
     * @param dorisType
     * @return hive primitive type info
     * @throws DdlException
     */
    private static PrimitiveTypeInfo convertToHiveColType(PrimitiveType dorisType) throws DdlException {
        switch (dorisType) {
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case TINYINT:
                return TypeInfoFactory.byteTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case INT:
                return TypeInfoFactory.intTypeInfo;
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case FLOAT:
                return TypeInfoFactory.floatTypeInfo;
            case DOUBLE:
                return TypeInfoFactory.doubleTypeInfo;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMALV2:
                return TypeInfoFactory.decimalTypeInfo;
            case DATE:
            case DATEV2:
                return TypeInfoFactory.dateTypeInfo;
            case DATETIME:
            case DATETIMEV2:
                return TypeInfoFactory.timestampTypeInfo;
            case CHAR:
                return TypeInfoFactory.charTypeInfo;
            case VARCHAR:
            case STRING:
                return TypeInfoFactory.varcharTypeInfo;
            default:
                throw new DdlException("Unsupported column type: " + dorisType);
        }
    }

    /**
     * Helper class for building a Hive expression.
     */
    public static class ExprBuilder {
        private final String tblName;
        private final Deque<ExprNodeDesc> queue = new LinkedList<>();

        public ExprBuilder(String tblName) {
            this.tblName = tblName;
        }

        public ExprNodeGenericFuncDesc build() throws DdlException {
            if (queue.size() != 1) {
                throw new DdlException("Build Hive expression Failed: " + queue.size());
            }
            return (ExprNodeGenericFuncDesc) queue.pollFirst();
        }

        public ExprBuilder pred(String name, int args) throws DdlException {
            return fn(name, TypeInfoFactory.booleanTypeInfo, args);
        }

        private ExprBuilder fn(String name, TypeInfo ti, int args) throws DdlException {
            List<ExprNodeDesc> children = new ArrayList<>();
            for (int i = 0; i < args; ++i) {
                children.add(queue.pollFirst());
            }
            try {
                queue.offerLast(new ExprNodeGenericFuncDesc(ti,
                        FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
            } catch (SemanticException e) {
                LOG.warn("Build Hive expression failed: semantic analyze exception: {}", e.getMessage());
                throw new DdlException("Build Hive expression Failed. Error: " + e.getMessage());
            }
            return this;
        }

        public ExprBuilder col(TypeInfo ti, String col) {
            queue.offerLast(new ExprNodeColumnDesc(ti, col, tblName, true));
            return this;
        }

        public ExprBuilder val(TypeInfo ti, Object val) {
            queue.offerLast(new ExprNodeConstantDesc(ti, val));
            return this;
        }
    }

    /**
     * Convert hive type to doris type.
     */
    public static Type hiveTypeToDorisType(String hiveType) {
        String lowerCaseType = hiveType.toLowerCase();
        switch (lowerCaseType) {
            case "boolean":
                return Type.BOOLEAN;
            case "tinyint":
                return Type.TINYINT;
            case "smallint":
                return Type.SMALLINT;
            case "int":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "date":
                return ScalarType.getDefaultDateType(Type.DATE);
            case "timestamp":
                return ScalarType.getDefaultDateType(Type.DATETIME);
            case "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "string":
            case "binary":
                return ScalarType.createStringType();
            default:
                break;
        }
        if (lowerCaseType.startsWith("array")) {
            if (lowerCaseType.indexOf("<") == 5 && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                Type innerType = hiveTypeToDorisType(lowerCaseType.substring(6, lowerCaseType.length() - 1));
                return ArrayType.create(innerType, true);
            }
        }
        if (lowerCaseType.startsWith("char")) {
            ScalarType type = ScalarType.createType(PrimitiveType.CHAR);
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                type.setLength(Integer.parseInt(match.group(1)));
            }
            return type;
        }
        if (lowerCaseType.startsWith("varchar")) {
            ScalarType type = ScalarType.createType(PrimitiveType.VARCHAR);
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                type.setLength(Integer.parseInt(match.group(1)));
            }
            return type;
        }
        if (lowerCaseType.startsWith("decimal")) {
            Matcher match = digitPattern.matcher(lowerCaseType);
            int precision = ScalarType.DEFAULT_PRECISION;
            int scale = ScalarType.DEFAULT_SCALE;
            if (match.find()) {
                precision = Integer.parseInt(match.group(1));
            }
            if (match.find()) {
                scale = Integer.parseInt(match.group(1));
            }
            return ScalarType.createDecimalType(precision, scale);
        }
        return Type.UNSUPPORTED;
    }

    public static String showCreateTable(org.apache.hadoop.hive.metastore.api.Table remoteTable) {
        StringBuilder output = new StringBuilder();
        if (remoteTable.isSetViewOriginalText() || remoteTable.isSetViewExpandedText()) {
            output.append(String.format("CREATE VIEW `%s` AS ", remoteTable.getTableName()));
            if (remoteTable.getViewExpandedText() != null) {
                output.append(remoteTable.getViewExpandedText());
            } else {
                output.append(remoteTable.getViewOriginalText());
            }
        } else {
            output.append(String.format("CREATE TABLE `%s`(\n", remoteTable.getTableName()));
            Iterator<FieldSchema> fields = remoteTable.getSd().getCols().iterator();
            while (fields.hasNext()) {
                FieldSchema field = fields.next();
                output.append(String.format("  `%s` %s", field.getName(), field.getType()));
                if (field.getComment() != null) {
                    output.append(String.format(" COMMENT '%s'", field.getComment()));
                }
                if (fields.hasNext()) {
                    output.append(",\n");
                }
            }
            output.append(")\n");
            if (remoteTable.getParameters().containsKey(COMMENT)) {
                output.append(String.format("COMMENT '%s'", remoteTable.getParameters().get(COMMENT))).append("\n");
            }
            if (remoteTable.getPartitionKeys().size() > 0) {
                output.append("PARTITIONED BY (\n")
                        .append(remoteTable.getPartitionKeys().stream().map(
                                        partition ->
                                                String.format(" `%s` `%s`", partition.getName(), partition.getType()))
                                .collect(Collectors.joining(",\n")))
                        .append(")\n");
            }
            StorageDescriptor descriptor = remoteTable.getSd();
            List<String> bucketCols = descriptor.getBucketCols();
            if (bucketCols != null && bucketCols.size() > 0) {
                output.append("CLUSTERED BY (\n")
                        .append(bucketCols.stream().map(
                                bucketCol -> "  " + bucketCol).collect(Collectors.joining(",\n")))
                        .append(")\n")
                        .append(String.format("INTO %d BUCKETS\n", descriptor.getNumBuckets()));
            }
            if (descriptor.getSerdeInfo().isSetSerializationLib()) {
                output.append("ROW FORMAT SERDE\n")
                        .append(String.format("  '%s'\n", descriptor.getSerdeInfo().getSerializationLib()));
            }
            if (descriptor.isSetInputFormat()) {
                output.append("STORED AS INPUTFORMAT\n")
                        .append(String.format("  '%s'\n", descriptor.getInputFormat()));
            }
            if (descriptor.isSetOutputFormat()) {
                output.append("OUTPUTFORMAT\n")
                        .append(String.format("  '%s'\n", descriptor.getOutputFormat()));
            }
            if (descriptor.isSetLocation()) {
                output.append("LOCATION\n")
                        .append(String.format("  '%s'\n", descriptor.getLocation()));
            }
            if (remoteTable.isSetParameters()) {
                output.append("TBLPROPERTIES (\n");
                Map<String, String> parameters = Maps.newHashMap();
                // Copy the parameters to a new Map to keep them unchanged.
                parameters.putAll(remoteTable.getParameters());
                if (parameters.containsKey(COMMENT)) {
                    // Comment is always added to the end of remote table parameters.
                    // It has already showed above in COMMENT section, so remove it here.
                    parameters.remove(COMMENT);
                }
                Iterator<Map.Entry<String, String>> params = parameters.entrySet().iterator();
                while (params.hasNext()) {
                    Map.Entry<String, String> param = params.next();
                    output.append(String.format("  '%s'='%s'", param.getKey(), param.getValue()));
                    if (params.hasNext()) {
                        output.append(",\n");
                    }
                }
                output.append(")");
            }
        }
        return output.toString();
    }

    public static org.apache.iceberg.Table getIcebergTable(HMSExternalTable table) {
        String metastoreUri = table.getMetastoreUri();
        org.apache.iceberg.hive.HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = getConfiguration(table);
        hiveCatalog.setConf(conf);
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(HMSResource.HIVE_METASTORE_URIS, metastoreUri);
        catalogProperties.put("uri", metastoreUri);
        hiveCatalog.initialize("hive", catalogProperties);

        return hiveCatalog.loadTable(TableIdentifier.of(table.getDbName(), table.getName()));
    }

    public static Configuration getConfiguration(HMSExternalTable table) {
        Configuration conf = new HdfsConfiguration(Config.load_default_conf_for_hms_client);
        for (Map.Entry<String, String> entry : table.getHadoopProperties().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }
}


