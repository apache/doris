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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Strings;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Helper class for HiveMetaStoreClient
 */
public class HiveMetaStoreClientHelper {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreClientHelper.class);

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

    public static HiveMetaStoreClient getClient(String metaStoreUris) throws DdlException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUris);
        HiveMetaStoreClient hivemetastoreclient = null;
        try {
            hivemetastoreclient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            LOG.warn("Create HiveMetaStoreClient failed: {}", e.getMessage());
            throw new DdlException("Create HiveMetaStoreClient failed");
        }
        return hivemetastoreclient;
    }

    /**
     * Check to see if the specified table exists in the specified database.
     * @param client HiveMetaStoreClient
     * @param dbName the specified database name
     * @param tblName the specified table name
     * @return TRUE if specified.tableName exists, FALSE otherwise.
     * @throws DdlException
     */
    public static boolean tableExists(HiveMetaStoreClient client, String dbName, String tblName) throws DdlException {
        try {
            return client.tableExists(dbName, tblName);
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed.");
        } finally {
            dropClient(client);
        }
    }

    /**
     * close connection to meta store
     */
    public static void dropClient(HiveMetaStoreClient client) {
        client.close();
    }

    /**
     * Get data files of partitions in hive table, filter by partition predicate
     * @param hiveTable
     * @param hivePartitionPredicate
     * @param fileStatuses
     * @param remoteHiveTbl
     * @return
     * @throws DdlException
     */
    public static String getHiveDataFiles(HiveTable hiveTable, ExprNodeGenericFuncDesc hivePartitionPredicate,
                                          List<TBrokerFileStatus> fileStatuses, Table remoteHiveTbl, StorageBackend.StorageType type) throws DdlException {
        HiveMetaStoreClient client = getClient(hiveTable.getHiveProperties().get(HiveTable.HIVE_METASTORE_URIS));

        List<RemoteIterator<LocatedFileStatus>> remoteIterators;
        Boolean onS3 = type.equals(StorageBackend.StorageType.S3);
        if (remoteHiveTbl.getPartitionKeys().size() > 0) {
            // hive partitioned table, get file iterator from table partition sd info
            List<Partition> hivePartitions = new ArrayList<>();
            try {
                client.listPartitionsByExpr(hiveTable.getHiveDb(), hiveTable.getHiveTable(),
                        SerializationUtilities.serializeExpressionToKryo(hivePartitionPredicate), null, (short) -1, hivePartitions);
            } catch (TException e) {
                LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
                throw new DdlException("Connect hive metastore failed.");
            } finally {
                client.close();
            }
            remoteIterators = getRemoteIterator(hivePartitions, hiveTable.getHiveProperties(), onS3);
        } else {
            // hive non-partitioned table, get file iterator from table sd info
            remoteIterators = getRemoteIterator(remoteHiveTbl, hiveTable.getHiveProperties(), onS3);
        }

        String hdfsUrl = "";
        for (RemoteIterator<LocatedFileStatus> iterator : remoteIterators) {
            try {
                while (iterator.hasNext()) {
                    LocatedFileStatus fileStatus = iterator.next();
                    TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus();
                    brokerFileStatus.setIsDir(fileStatus.isDirectory());
                    brokerFileStatus.setIsSplitable(true);
                    brokerFileStatus.setSize(fileStatus.getLen());
                    // path = "/path/to/partition/file_name"
                    // eg: /home/work/dev/hive/apache-hive-2.3.7-bin/data/warehouse/dae.db/customer/state=CA/city=SanJose/000000_0
                    String path = fileStatus.getPath().toUri().getPath();
                    if (onS3) {
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
                throw new DdlException("List HDFS file failed.");
            }
        }

        return hdfsUrl;
    }

    private static void setS3Configuration(Configuration configuration, Map<String, String> properties) {
        if (properties.containsKey(HiveTable.S3_AK)) {
            configuration.set("fs.s3a.access.key", properties.get(HiveTable.S3_AK));
        }
        if (properties.containsKey(HiveTable.S3_SK)) {
            configuration.set("fs.s3a.secret.key", properties.get(HiveTable.S3_SK));
        }
        if (properties.containsKey(HiveTable.S3_ENDPOINT)) {
            configuration.set("fs.s3a.endpoint", properties.get(HiveTable.S3_ENDPOINT));
        }
        configuration.set("fs.s3.impl.disable.cache", "true");
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.attempts.maximum", "2");
    }

    private static List<RemoteIterator<LocatedFileStatus>> getRemoteIterator(
            List<Partition> partitions, Map<String, String> properties, boolean onS3)
            throws DdlException {
        List<RemoteIterator<LocatedFileStatus>> iterators = new ArrayList<>();
        Configuration configuration = new Configuration(false);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!entry.getKey().equals(HiveTable.HIVE_METASTORE_URIS)) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        if (onS3) {
            setS3Configuration(configuration, properties);
        }
        for (Partition p : partitions) {
            String location = p.getSd().getLocation();
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
            try {
                FileSystem fileSystem = path.getFileSystem(configuration);
                iterators.add(fileSystem.listLocatedStatus(path));
            } catch (IOException e) {
                LOG.warn("Get HDFS file remote iterator failed. {}", e.getMessage());
                throw new DdlException("Get HDFS file remote iterator failed.");
            }
        }
        return iterators;
    }

    private static List<RemoteIterator<LocatedFileStatus>> getRemoteIterator(
            Table table, Map<String, String> properties, boolean onS3)
            throws DdlException {
        List<RemoteIterator<LocatedFileStatus>> iterators = new ArrayList<>();
        Configuration configuration = new Configuration(false);
        boolean isSecurityEnabled = false;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!entry.getKey().equals(HiveTable.HIVE_METASTORE_URIS)) {
                configuration.set(entry.getKey(), entry.getValue());
            }
            if (entry.getKey().equals(BrokerUtil.HADOOP_SECURITY_AUTHENTICATION)
                && entry.getValue().equals(AuthType.KERBEROS.getDesc())) {
                isSecurityEnabled = true;
            }
        }
        if (onS3) {
            setS3Configuration(configuration, properties);
        }
        String location = table.getSd().getLocation();
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
        try {
            if (isSecurityEnabled) {
                UserGroupInformation.setConfiguration(configuration);
                // login user from keytab
                UserGroupInformation.loginUserFromKeytab(properties.get(BrokerUtil.HADOOP_KERBEROS_PRINCIPAL),
                    properties.get(BrokerUtil.HADOOP_KERBEROS_KEYTAB));
            }
            FileSystem fileSystem = path.getFileSystem(configuration);
            iterators.add(fileSystem.listLocatedStatus(path));
        } catch (IOException e) {
            LOG.warn("Get HDFS file remote iterator failed. {}" + e.getMessage());
            throw new DdlException("Get HDFS file remote iterator failed.");
        }
        return iterators;
    }

    public static List<String> getPartitionNames(HiveTable hiveTable) throws DdlException {
        HiveMetaStoreClient client = getClient(hiveTable.getHiveProperties().get(HiveTable.HIVE_METASTORE_URIS));
        List<String> partitionNames = new ArrayList<>();
        try {
            partitionNames = client.listPartitionNames(hiveTable.getHiveDb(), hiveTable.getHiveTable(), (short) -1);
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed.");
        }

        return partitionNames;
    }

    public static Table getTable(HiveTable hiveTable) throws DdlException {
        HiveMetaStoreClient client = getClient(hiveTable.getHiveProperties().get(HiveTable.HIVE_METASTORE_URIS));
        Table table;
        try {
            table = client.getTable(hiveTable.getHiveDb(), hiveTable.getHiveTable());
        } catch (TException e) {
            LOG.warn("Hive metastore thrift exception: {}", e.getMessage());
            throw new DdlException("Connect hive metastore failed.");
        }
        return table;
    }

    /**
     * Convert Doris expr to Hive expr, only for partition column
     * @param dorisExpr
     * @param partitions
     * @param tblName
     * @return
     * @throws DdlException
     * @throws SemanticException
     */
    public static ExprNodeGenericFuncDesc convertToHivePartitionExpr(Expr dorisExpr, List<String> partitions, String tblName) throws DdlException {
        if (dorisExpr == null) {
            return null;
        }

        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    ExprNodeGenericFuncDesc left = convertToHivePartitionExpr(compoundPredicate.getChild(0), partitions, tblName);
                    ExprNodeGenericFuncDesc right = convertToHivePartitionExpr(compoundPredicate.getChild(0), partitions, tblName);
                    if (left != null && right != null) {
                        List<ExprNodeDesc> andArgs = new ArrayList<>();
                        andArgs.add(left);
                        andArgs.add(right);
                        return getCompoundExpr(andArgs, "and");
                    } else if (left != null && right == null) {
                        return left;
                    } else if (left == null && right != null) {
                        return right;
                    }
                    return null;
                }
                case OR: {
                    ExprNodeGenericFuncDesc left = convertToHivePartitionExpr(compoundPredicate.getChild(0), partitions, tblName);
                    ExprNodeGenericFuncDesc right = convertToHivePartitionExpr(compoundPredicate.getChild(0), partitions, tblName);
                    if (left != null && right != null) {
                        List<ExprNodeDesc> orArgs = new ArrayList<>();
                        orArgs.add(left);
                        orArgs.add(right);
                        return getCompoundExpr(orArgs, "or");
                    } else if (left != null && right == null) {
                        return left;
                    } else if (left == null && right != null) {
                        return right;
                    }
                    return null;
                }
                default:
                    return null;
            }
        }

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
                SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                LiteralExpr literalExpr = null;
                if (slotRef == null && eq.getChild(0).isLiteral()) {
                    literalExpr = (LiteralExpr) eq.getChild(0);
                    slotRef = convertDorisExprToSlotRef(eq.getChild(1));
                } else if (eq.getChild(1).isLiteral()) {
                    literalExpr = (LiteralExpr) eq.getChild(1);
                }
                if (slotRef == null || literalExpr == null) {
                    return null;
                }
                String colName = slotRef.getColumnName();
                // check whether colName is partition column or not
                if (!partitions.contains(colName)) {
                    return null;
                }
                PrimitiveType dorisPrimitiveType = slotRef.getType().getPrimitiveType();
                PrimitiveTypeInfo hivePrimitiveType = convertToHiveColType(dorisPrimitiveType);
                Object value = extractDorisLiteral(literalExpr);
                ExprBuilder exprBuilder = new ExprBuilder(tblName);
                if (value == null) {
                    if (opcode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, "NULL")
                                .pred("=", 2).build();
                    } else {
                        return null;
                    }
                }
                switch (opcode) {
                    case EQ:
                    case EQ_FOR_NULL:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred("=", 2).build();
                    case NE:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred("!=", 2).build();
                    case GE:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred(">=", 2).build();
                    case GT:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred(">", 2).build();
                    case LE:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred("<=", 2).build();
                    case LT:
                        return exprBuilder.col(hivePrimitiveType, colName)
                                .val(hivePrimitiveType, value)
                                .pred("<", 2).build();
                    default:
                        return null;
                }
            default:
                return null;
        }

    }

    public static ExprNodeGenericFuncDesc getCompoundExpr(List<ExprNodeDesc> args, String op) throws DdlException {
        ExprNodeGenericFuncDesc compoundExpr;
        try {
            compoundExpr = ExprNodeGenericFuncDesc.newInstance(
                    FunctionRegistry.getFunctionInfo(op).getGenericUDF(), args);
        } catch (SemanticException e) {
            LOG.warn("Convert to Hive expr failed: {}", e.getMessage());
            throw new DdlException("Convert to Hive expr failed.");
        }
        return compoundExpr;
    }

    private static SlotRef convertDorisExprToSlotRef(Expr expr) {
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

    private static Object extractDorisLiteral(Expr expr) {
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
            case DECIMALV2:
                return TypeInfoFactory.decimalTypeInfo;
            case DATE:
                return TypeInfoFactory.dateTypeInfo;
            case DATETIME:
                return TypeInfoFactory.timestampTypeInfo;
            case CHAR:
                return TypeInfoFactory.charTypeInfo;
            case VARCHAR:
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
        private final Stack<ExprNodeDesc> stack = new Stack<>();

        public ExprBuilder(String tblName) {
            this.tblName = tblName;
        }

        public ExprNodeGenericFuncDesc build() throws DdlException {
            if (stack.size() != 1) {
                throw new DdlException("Build Hive expression Failed: " + stack.size());
            }
            return (ExprNodeGenericFuncDesc)stack.pop();
        }

        public ExprBuilder pred(String name, int args) throws DdlException {
            return fn(name, TypeInfoFactory.booleanTypeInfo, args);
        }

        private ExprBuilder fn(String name, TypeInfo ti, int args) throws DdlException {
            List<ExprNodeDesc> children = new ArrayList<>();
            for (int i = 0; i < args; ++i) {
                children.add(stack.pop());
            }
            try {
                stack.push(new ExprNodeGenericFuncDesc(ti,
                        FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
            } catch (SemanticException e) {
                LOG.warn("Build Hive expression failed: semantic analyze exception: {}", e.getMessage());
                throw new DdlException("Build Hive expression Failed");
            }
            return this;
        }

        public ExprBuilder col(TypeInfo ti, String col) {
            stack.push(new ExprNodeColumnDesc(ti, col, tblName, true));
            return this;
        }

        public ExprBuilder val(TypeInfo ti, Object val) {
            stack.push(new ExprNodeConstantDesc(ti, val));
            return this;
        }
    }
}
