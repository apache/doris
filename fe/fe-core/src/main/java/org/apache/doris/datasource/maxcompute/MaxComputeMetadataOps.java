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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * MaxCompute metadata operations for DDL support (CREATE TABLE, etc.)
 */
public class MaxComputeMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(MaxComputeMetadataOps.class);

    private static final long MAX_LIFECYCLE_DAYS = 37231;
    private static final int MAX_BUCKET_NUM = 1024;

    private final MaxComputeExternalCatalog dorisCatalog;
    private final Odps odps;

    public MaxComputeMetadataOps(MaxComputeExternalCatalog dorisCatalog, Odps odps) {
        this.dorisCatalog = dorisCatalog;
        this.odps = odps;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return dorisCatalog.tableExist(null, dbName, tblName);
    }

    @Override
    public boolean databaseExist(String dbName) {
        return dorisCatalog.getMcStructureHelper().databaseExist(dorisCatalog.getClient(), dbName);
    }

    @Override
    public List<String> listDatabaseNames() {
        return dorisCatalog.listDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return dorisCatalog.listTableNames(null, dbName);
    }

    // ==================== Create/Drop Database ====================

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        ExternalDatabase<?> dorisDb = dorisCatalog.getDbNullable(dbName);
        boolean exists = databaseExist(dbName);
        if (dorisDb != null || exists) {
            if (ifNotExists) {
                LOG.info("create database[{}] which already exists", dbName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
        }
        dorisCatalog.getMcStructureHelper().createDb(odps, dbName, ifNotExists);
        return false;
    }

    @Override
    public void afterCreateDb() {
        dorisCatalog.resetMetaCacheNames();
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        ExternalDatabase<?> dorisDb = dorisCatalog.getDbNullable(dbName);
        if (dorisDb == null) {
            if (ifExists) {
                LOG.info("drop database[{}] which does not exist", dbName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        if (force) {
            List<String> remoteTableNames = listTableNames(dorisDb.getRemoteName());
            for (String remoteTableName : remoteTableNames) {
                ExternalTable tbl = null;
                try {
                    tbl = (ExternalTable) dorisDb.getTableOrDdlException(remoteTableName);
                } catch (DdlException e) {
                    LOG.warn("failed to get table when force drop database [{}], table[{}], error: {}",
                            dbName, remoteTableName, e.getMessage());
                    continue;
                }
                dropTableImpl(tbl, true);
            }
        }
        dorisCatalog.getMcStructureHelper().dropDb(odps, dbName, ifExists);
    }

    @Override
    public void afterDropDb(String dbName) {
        dorisCatalog.unregisterDatabase(dbName);
    }

    // ==================== Create Table ====================

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        String dbName = createTableInfo.getDbName();
        String tableName = createTableInfo.getTableName();

        // 1. Validate database existence
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException(
                    "Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }

        // 2. Check if table exists in remote
        if (tableExist(db.getRemoteName(), tableName)) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        // 3. Check if table exists in local (case sensitivity issue)
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable != null) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        // 4. Validate columns
        List<Column> columns = createTableInfo.getColumns();
        validateColumns(columns);

        // 5. Validate partition description
        PartitionDesc partitionDesc = createTableInfo.getPartitionDesc();
        validatePartitionDesc(partitionDesc);

        // 6. Build MaxCompute TableSchema
        TableSchema schema = buildMaxComputeTableSchema(columns, partitionDesc);

        // 7. Extract properties
        Map<String, String> properties = createTableInfo.getProperties();
        Long lifecycle = extractLifecycle(properties);
        Map<String, String> mcProperties = extractMaxComputeProperties(properties);
        Integer bucketNum = extractBucketNum(createTableInfo);

        // 8. Create table via MaxCompute SDK
        McStructureHelper structureHelper = dorisCatalog.getMcStructureHelper();
        Tables.TableCreator creator = structureHelper.createTableCreator(
                odps, db.getRemoteName(), tableName, schema);

        if (createTableInfo.isIfNotExists()) {
            creator.ifNotExists();
        }

        String comment = createTableInfo.getComment();
        if (comment != null && !comment.isEmpty()) {
            creator.withComment(comment);
        }

        if (lifecycle != null) {
            creator.withLifeCycle(lifecycle);
        }

        if (!mcProperties.isEmpty()) {
            creator.withTblProperties(mcProperties);
        }

        if (bucketNum != null) {
            creator.withDeltaTableBucketNum(bucketNum);
        }

        try {
            creator.create();
        } catch (OdpsException e) {
            throw new DdlException("Failed to create MaxCompute table '" + tableName + "': " + e.getMessage(), e);
        }

        return false;
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().resetMetaCacheNames();
        }
        LOG.info("after create table {}.{}.{}, is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    // ==================== Drop Table (not supported yet) ====================

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        // Get remote names (handles case-sensitivity)
        String remoteDbName = dorisTable.getRemoteDbName();
        String remoteTblName = dorisTable.getRemoteName();

        // Check table existence
        if (!tableExist(remoteDbName, remoteTblName)) {
            if (ifExists) {
                LOG.info("drop table[{}.{}] which does not exist", remoteDbName, remoteTblName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE,
                        remoteTblName, remoteDbName);
            }
        }

        // Drop table via McStructureHelper
        try {
            McStructureHelper structureHelper = dorisCatalog.getMcStructureHelper();
            structureHelper.dropTable(odps, remoteDbName, remoteTblName, ifExists);
            LOG.info("Successfully dropped MaxCompute table: {}.{}", remoteDbName, remoteTblName);
        } catch (OdpsException e) {
            throw new DdlException("Failed to drop MaxCompute table '"
                    + remoteTblName + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void afterDropTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().unregisterTable(tblName);
        }
        LOG.info("after drop table {}.{}.{}, is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException {
        throw new DdlException("Truncate table is not supported for MaxCompute catalog.");
    }

    // ==================== Branch/Tag (not supported) ====================

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UserException("Branch operations are not supported for MaxCompute catalog.");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        throw new UserException("Tag operations are not supported for MaxCompute catalog.");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UserException("Tag operations are not supported for MaxCompute catalog.");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UserException("Branch operations are not supported for MaxCompute catalog.");
    }

    // ==================== Type Conversion ====================

    /**
     * Convert Doris type to MaxCompute TypeInfo.
     */
    public static TypeInfo dorisTypeToMcType(Type dorisType) throws UserException {
        if (dorisType.isScalarType()) {
            return dorisScalarTypeToMcType(dorisType);
        } else if (dorisType.isArrayType()) {
            ArrayType arrayType = (ArrayType) dorisType;
            TypeInfo elementType = dorisTypeToMcType(arrayType.getItemType());
            return TypeInfoFactory.getArrayTypeInfo(elementType);
        } else if (dorisType.isMapType()) {
            MapType mapType = (MapType) dorisType;
            TypeInfo keyType = dorisTypeToMcType(mapType.getKeyType());
            TypeInfo valueType = dorisTypeToMcType(mapType.getValueType());
            return TypeInfoFactory.getMapTypeInfo(keyType, valueType);
        } else if (dorisType.isStructType()) {
            StructType structType = (StructType) dorisType;
            List<StructField> fields = structType.getFields();
            List<String> fieldNames = new ArrayList<>(fields.size());
            List<TypeInfo> fieldTypes = new ArrayList<>(fields.size());
            for (StructField field : fields) {
                fieldNames.add(field.getName());
                fieldTypes.add(dorisTypeToMcType(field.getType()));
            }
            return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypes);
        } else {
            throw new UserException("Unsupported Doris type for MaxCompute: " + dorisType);
        }
    }

    private static TypeInfo dorisScalarTypeToMcType(Type dorisType) throws UserException {
        PrimitiveType primitiveType = dorisType.getPrimitiveType();
        switch (primitiveType) {
            case BOOLEAN:
                return TypeInfoFactory.BOOLEAN;
            case TINYINT:
                return TypeInfoFactory.TINYINT;
            case SMALLINT:
                return TypeInfoFactory.SMALLINT;
            case INT:
                return TypeInfoFactory.INT;
            case BIGINT:
                return TypeInfoFactory.BIGINT;
            case FLOAT:
                return TypeInfoFactory.FLOAT;
            case DOUBLE:
                return TypeInfoFactory.DOUBLE;
            case CHAR:
                return TypeInfoFactory.getCharTypeInfo(((ScalarType) dorisType).getLength());
            case VARCHAR:
                return TypeInfoFactory.getVarcharTypeInfo(((ScalarType) dorisType).getLength());
            case STRING:
                return TypeInfoFactory.STRING;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return TypeInfoFactory.getDecimalTypeInfo(
                        ((ScalarType) dorisType).getScalarPrecision(),
                        ((ScalarType) dorisType).getScalarScale());
            case DATE:
            case DATEV2:
                return TypeInfoFactory.DATE;
            case DATETIME:
            case DATETIMEV2:
                return TypeInfoFactory.DATETIME;
            case LARGEINT:
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
            case AGG_STATE:
            case JSONB:
            case VARIANT:
            case IPV4:
            case IPV6:
            default:
                throw new UserException(
                        "Unsupported Doris type for MaxCompute: " + primitiveType);
        }
    }

    // ==================== Validation ====================

    private void validateColumns(List<Column> columns) throws UserException {
        if (columns == null || columns.isEmpty()) {
            throw new UserException("Table must have at least one column.");
        }
        Set<String> columnNames = new HashSet<>();
        for (Column col : columns) {
            if (col.isAutoInc()) {
                throw new UserException(
                        "Auto-increment columns are not supported for MaxCompute tables: " + col.getName());
            }
            if (col.isAggregated()) {
                throw new UserException(
                        "Aggregation columns are not supported for MaxCompute tables: " + col.getName());
            }
            String lowerName = col.getName().toLowerCase();
            if (!columnNames.add(lowerName)) {
                throw new UserException("Duplicate column name: " + col.getName());
            }
            // Validate that the type is convertible
            dorisTypeToMcType(col.getType());
        }
    }

    private void validatePartitionDesc(PartitionDesc partitionDesc) throws UserException {
        if (partitionDesc == null) {
            return;
        }
        ArrayList<Expr> exprs = partitionDesc.getPartitionExprs();
        if (exprs == null || exprs.isEmpty()) {
            return;
        }
        for (Expr expr : exprs) {
            if (expr instanceof SlotRef) {
                // Identity partition - OK
            } else if (expr instanceof FunctionCallExpr) {
                String funcName = ((FunctionCallExpr) expr).getFnName().getFunction();
                throw new UserException(
                        "MaxCompute does not support partition transform '" + funcName
                                + "'. Only identity partitions are supported.");
            } else {
                throw new UserException("Invalid partition expression: " + expr.toSql());
            }
        }
    }

    // ==================== Schema Building ====================

    private TableSchema buildMaxComputeTableSchema(List<Column> columns, PartitionDesc partitionDesc)
            throws UserException {
        Set<String> partitionColNames = new HashSet<>();
        if (partitionDesc != null && partitionDesc.getPartitionColNames() != null) {
            for (String name : partitionDesc.getPartitionColNames()) {
                partitionColNames.add(name.toLowerCase());
            }
        }

        TableSchema schema = new TableSchema();

        // Add regular columns (non-partition)
        for (Column col : columns) {
            if (!partitionColNames.contains(col.getName().toLowerCase())) {
                TypeInfo mcType = dorisTypeToMcType(col.getType());
                com.aliyun.odps.Column mcCol = new com.aliyun.odps.Column(
                        col.getName(), mcType, col.getComment());
                schema.addColumn(mcCol);
            }
        }

        // Add partition columns in the order specified by partitionDesc
        if (partitionDesc != null && partitionDesc.getPartitionColNames() != null) {
            for (String partColName : partitionDesc.getPartitionColNames()) {
                Column col = findColumnByName(columns, partColName);
                if (col == null) {
                    throw new UserException("Partition column '" + partColName + "' not found in column definitions.");
                }
                TypeInfo mcType = dorisTypeToMcType(col.getType());
                com.aliyun.odps.Column mcCol = new com.aliyun.odps.Column(
                        col.getName(), mcType, col.getComment());
                schema.addPartitionColumn(mcCol);
            }
        }

        return schema;
    }

    private Column findColumnByName(List<Column> columns, String name) {
        for (Column col : columns) {
            if (col.getName().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null;
    }

    // ==================== Property Extraction ====================

    private Long extractLifecycle(Map<String, String> properties) throws UserException {
        String lifecycleStr = properties.get("mc.lifecycle");
        if (lifecycleStr == null) {
            lifecycleStr = properties.get("lifecycle");
        }
        if (lifecycleStr != null) {
            try {
                long lifecycle = Long.parseLong(lifecycleStr);
                if (lifecycle <= 0 || lifecycle > MAX_LIFECYCLE_DAYS) {
                    throw new UserException(
                            "Invalid lifecycle value: " + lifecycle
                                    + ". Must be between 1 and " + MAX_LIFECYCLE_DAYS + ".");
                }
                return lifecycle;
            } catch (NumberFormatException e) {
                throw new UserException("Invalid lifecycle value: '" + lifecycleStr + "'. Must be a positive integer.");
            }
        }
        return null;
    }

    private Map<String, String> extractMaxComputeProperties(Map<String, String> properties) {
        Map<String, String> mcProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("mc.tblproperty.")) {
                String mcKey = entry.getKey().substring("mc.tblproperty.".length());
                mcProperties.put(mcKey, entry.getValue());
            }
        }
        return mcProperties;
    }

    private Integer extractBucketNum(CreateTableInfo createTableInfo) throws UserException {
        DistributionDesc distributionDesc = createTableInfo.getDistributionDesc();
        if (distributionDesc == null) {
            return null;
        }
        if (!(distributionDesc instanceof HashDistributionDesc)) {
            throw new UserException(
                    "MaxCompute only supports hash distribution. Got: " + distributionDesc.getClass().getSimpleName());
        }

        HashDistributionDesc hashDist = (HashDistributionDesc) distributionDesc;
        int bucketNum = hashDist.getBuckets();

        if (bucketNum <= 0 || bucketNum > MAX_BUCKET_NUM) {
            throw new UserException(
                    "Invalid bucket number: " + bucketNum + ". Must be between 1 and " + MAX_BUCKET_NUM + ".");
        }

        return bucketNum;
    }
}
