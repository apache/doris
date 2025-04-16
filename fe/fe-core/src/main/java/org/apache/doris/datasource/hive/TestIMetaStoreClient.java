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

package org.apache.doris.datasource.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// Only for unit test
public class TestIMetaStoreClient implements IMetaStoreClient {
    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        return false;
    }

    @Override
    public void setHiveAddedJars(String s) {

    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    @Override
    public void reconnect() throws MetaException {

    }

    @Override
    public void close() {

    }

    @Override
    public void setMetaConf(String s, String s1) throws MetaException, TException {

    }

    @Override
    public String getMetaConf(String s) throws MetaException, TException {
        return "";
    }

    @Override
    public void createCatalog(Catalog catalog)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterCatalog(String s, Catalog catalog)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public Catalog getCatalog(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public void dropCatalog(String s)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public List<String> getDatabases(String s) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getDatabases(String s, String s1) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getAllDatabases(String s) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getTables(String s, String s1) throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getTables(String s, String s1, String s2) throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getTables(String s, String s1, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getTables(String s, String s1, String s2, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s, String s1)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<TableMeta> getTableMeta(String s, String s1, List<String> list)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<TableMeta> getTableMeta(String s, String s1, String s2, List<String> list)
            throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getAllTables(String s) throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getAllTables(String s, String s1) throws MetaException, TException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listTableNamesByFilter(String s, String s1, short i)
            throws TException, InvalidOperationException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listTableNamesByFilter(String s, String s1, String s2, int i)
            throws TException, InvalidOperationException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public void dropTable(String s, String s1, boolean b, boolean b1)
            throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String s, String s1, boolean b, boolean b1, boolean b2)
            throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String s, String s1) throws MetaException, TException, NoSuchObjectException {

    }

    @Override
    public void dropTable(String s, String s1, String s2, boolean b, boolean b1, boolean b2)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void truncateTable(String s, String s1, List<String> list) throws MetaException, TException {

    }

    @Override
    public void truncateTable(String s, String s1, String s2, List<String> list) throws MetaException, TException {

    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean tableExists(String s, String s1) throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public boolean tableExists(String s, String s1, String s2) throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public Database getDatabase(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Database getDatabase(String s, String s1) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Table getTable(String s, String s1) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public Table getTable(String s, String s1, String s2) throws MetaException, TException {
        return null;
    }

    @Override
    public List<Table> getTableObjectsByName(String s, List<String> list)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Table> getTableObjectsByName(String s, String s1, List<String> list)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return Lists.newArrayList();
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String s)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public void updateCreationMetadata(String s, String s1, CreationMetadata creationMetadata)
            throws MetaException, TException {

    }

    @Override
    public void updateCreationMetadata(String s, String s1, String s2, CreationMetadata creationMetadata)
            throws MetaException, TException {

    }

    @Override
    public Partition appendPartition(String s, String s1, List<String> list)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition appendPartition(String s, String s1, String s2, List<String> list)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition appendPartition(String s, String s1, String s2)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition appendPartition(String s, String s1, String s2, String s3)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public int add_partitions(List<Partition> list)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return 0;
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return 0;
    }

    @Override
    public List<Partition> add_partitions(List<Partition> list, boolean b, boolean b1)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public Partition getPartition(String s, String s1, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2, String s3)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2, String s3, String s4,
            String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1, String s2, String s3)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1, String s2, String s3,
            String s4, String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public Partition getPartition(String s, String s1, String s2)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, String s3)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Partition getPartitionWithAuthInfo(String s, String s1, List<String> list, String s2, List<String> list1)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Partition getPartitionWithAuthInfo(String s, String s1, String s2, List<String> list, String s3,
            List<String> list1) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, short i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, String s2, int i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, List<String> list, short i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, String s2, List<String> list, int i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, short i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, int i)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, List<String> list, short i)
            throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, List<String> list, int i)
            throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest)
            throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public int getNumPartitionsByFilter(String s, String s1, String s2)
            throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public int getNumPartitionsByFilter(String s, String s1, String s2, String s3)
            throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public List<Partition> listPartitionsByFilter(String s, String s1, String s2, short i)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitionsByFilter(String s, String s1, String s2, String s3, int i)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, String s3, int i)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public boolean listPartitionsByExpr(String s, String s1, byte[] bytes, String s2, short i, List<Partition> list)
            throws TException {
        return false;
    }

    @Override
    public boolean listPartitionsByExpr(String s, String s1, String s2, byte[] bytes, String s3, int i,
            List<Partition> list) throws TException {
        return false;
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, short i, String s2, List<String> list)
            throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, String s2, int i, String s3,
            List<String> list) throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> getPartitionsByNames(String s, String s1, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> getPartitionsByNames(String s, String s1, String s2, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, List<String> list, short i, String s2,
            List<String> list1) throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, String s2, List<String> list, int i,
            String s3, List<String> list1) throws MetaException, TException, NoSuchObjectException {
        return Lists.newArrayList();
    }

    @Override
    public void markPartitionForEvent(String s, String s1, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public void markPartitionForEvent(String s, String s1, String s2, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, String s2, Map<String, String> map,
            PartitionEventType partitionEventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public void validatePartitionNameCharacters(List<String> list) throws TException, MetaException {

    }

    @Override
    public void createTable(Table table)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void alter_table(String s, String s1, Table table)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String s, String s1, String s2, Table table, EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String s, String s1, Table table, boolean b)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table_with_environmentContext(String s, String s1, Table table,
            EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String s)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1, boolean b2)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String s, String s1, boolean b, boolean b1, boolean b2)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alterDatabase(String s, Database database) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public void alterDatabase(String s, String s1, Database database)
            throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean dropPartition(String s, String s1, List<String> list, boolean b)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list, boolean b)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, List<String> list, PartitionDropOptions partitionDropOptions)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list,
            PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list, boolean b,
            boolean b1) throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list, boolean b,
            boolean b1, boolean b2) throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list,
            PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<Partition> dropPartitions(String s, String s1, String s2, List<ObjectPair<Integer, byte[]>> list,
            PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, boolean b)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, String s3, boolean b)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public void alter_partition(String s, String s1, Partition partition)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partition(String s, String s1, Partition partition, EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partition(String s, String s1, String s2, Partition partition,
            EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String s, String s1, List<Partition> list)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String s, String s1, List<Partition> list, EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(String s, String s1, String s2, List<Partition> list,
            EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void renamePartition(String s, String s1, List<String> list, Partition partition)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void renamePartition(String s, String s1, String s2, List<String> list, Partition partition)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public List<FieldSchema> getFields(String s, String s1)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<FieldSchema> getFields(String s, String s1, String s2)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<FieldSchema> getSchema(String s, String s1)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public List<FieldSchema> getSchema(String s, String s1, String s2)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return Lists.newArrayList();
    }

    @Override
    public String getConfigValue(String s, String s1) throws TException, ConfigValSecurityException {
        return "";
    }

    @Override
    public List<String> partitionNameToVals(String s) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public Map<String, String> partitionNameToSpec(String s) throws MetaException, TException {
        return Maps.newHashMap();
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String s, String s1, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String s, String s1, String s2, List<String> list)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String s, String s1, List<String> list,
            List<String> list1) throws NoSuchObjectException, MetaException, TException {
        return Maps.newHashMap();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String s, String s1, String s2,
            List<String> list, List<String> list1) throws NoSuchObjectException, MetaException, TException {
        return Maps.newHashMap();
    }

    @Override
    public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3, String s4)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String s, String s1, String s2)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String s, String s1, String s2, String s3)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean drop_role(String s) throws MetaException, TException {
        return false;
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public boolean grant_role(String s, String s1, PrincipalType principalType, String s2, PrincipalType principalType1,
            boolean b) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
            throws MetaException, TException {
        return false;
    }

    @Override
    public List<Role> list_roles(String s, PrincipalType principalType) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s, List<String> list)
            throws MetaException, TException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag)
            throws MetaException, TException {
        return false;
    }

    @Override
    public String getDelegationToken(String s, String s1) throws MetaException, TException {
        return "";
    }

    @Override
    public long renewDelegationToken(String s) throws MetaException, TException {
        return 0;
    }

    @Override
    public void cancelDelegationToken(String s) throws MetaException, TException {

    }

    @Override
    public String getTokenStrForm() throws IOException {
        return "";
    }

    @Override
    public boolean addToken(String s, String s1) throws TException {
        return false;
    }

    @Override
    public boolean removeToken(String s) throws TException {
        return false;
    }

    @Override
    public String getToken(String s) throws TException {
        return "";
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return Lists.newArrayList();
    }

    @Override
    public int addMasterKey(String s) throws MetaException, TException {
        return 0;
    }

    @Override
    public void updateMasterKey(Integer integer, String s) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public boolean removeMasterKey(Integer integer) throws TException {
        return false;
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return new String[0];
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterFunction(String s, String s1, Function function)
            throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterFunction(String s, String s1, String s2, Function function)
            throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropFunction(String s, String s1)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public void dropFunction(String s, String s1, String s2)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public Function getFunction(String s, String s1) throws MetaException, TException {
        return null;
    }

    @Override
    public Function getFunction(String s, String s1, String s2) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> getFunctions(String s, String s1) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<String> getFunctions(String s, String s1, String s2) throws MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns(long l) throws TException {
        return null;
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String s) throws TException {
        return null;
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> list, String s) throws TException {
        return Lists.newArrayList();
    }

    @Override
    public long openTxn(String s) throws TException {
        return 0;
    }

    @Override
    public List<Long> replOpenTxn(String s, List<Long> list, String s1) throws TException {
        return Lists.newArrayList();
    }

    @Override
    public OpenTxnsResponse openTxns(String s, int i) throws TException {
        return null;
    }

    @Override
    public void rollbackTxn(long l) throws NoSuchTxnException, TException {

    }

    @Override
    public void replRollbackTxn(long l, String s) throws NoSuchTxnException, TException {

    }

    @Override
    public void commitTxn(long l) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void replCommitTxn(long l, String s) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> list) throws TException {

    }

    @Override
    public long allocateTableWriteId(long l, String s, String s1) throws TException {
        return 0;
    }

    @Override
    public void replTableWriteIdState(String s, String s1, String s2, List<String> list) throws TException {

    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> list, String s, String s1) throws TException {
        return Lists.newArrayList();
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String s, String s1, String s2, List<TxnToWriteId> list)
            throws TException {
        return Lists.newArrayList();
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return null;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return null;
    }

    @Override
    public LockResponse checkLock(long l)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return null;
    }

    @Override
    public void unlock(long l) throws NoSuchLockException, TxnOpenException, TException {

    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        return null;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return null;
    }

    @Override
    public void heartbeat(long l, long l1)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long l, long l1) throws TException {
        return null;
    }

    @Override
    public void compact(String s, String s1, String s2, CompactionType compactionType) throws TException {

    }

    @Override
    public void compact(String s, String s1, String s2, CompactionType compactionType, Map<String, String> map)
            throws TException {

    }

    @Override
    public CompactionResponse compact2(String s, String s1, String s2, CompactionType compactionType,
            Map<String, String> map) throws TException {
        return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return null;
    }

    @Override
    public void addDynamicPartitions(long l, long l1, String s, String s1, List<String> list) throws TException {

    }

    @Override
    public void addDynamicPartitions(long l, long l1, String s, String s1, List<String> list,
            DataOperationType dataOperationType) throws TException {

    }

    @Override
    public void insertTable(Table table, boolean b) throws MetaException {

    }

    @Override
    public NotificationEventResponse getNextNotification(long l, int i, NotificationFilter notificationFilter)
            throws TException {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return null;
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(
            NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
        return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return null;
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincipalsInRoleRequest)
            throws MetaException, TException {
        return null;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest) throws MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String s, String s1, List<String> list, List<String> list1)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String s, String s1, String s2, List<String> list, List<String> list1)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest setPartitionsStatsRequest)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return false;
    }

    @Override
    public void flushCache() {

    }

    @Override
    public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> list) throws TException {
        return null;
    }

    @Override
    public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> list, ByteBuffer byteBuffer,
            boolean b) throws TException {
        return null;
    }

    @Override
    public void clearFileMetadata(List<Long> list) throws TException {

    }

    @Override
    public void putFileMetadata(List<Long> list, List<ByteBuffer> list1) throws TException {

    }

    @Override
    public boolean isSameConfObj(Configuration configuration) {
        return false;
    }

    @Override
    public boolean cacheFileMetadata(String s, String s1, String s2, boolean b) throws TException {
        return false;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException {
        return Lists.newArrayList();
    }

    @Override
    public void createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1,
            List<SQLUniqueConstraint> list2, List<SQLNotNullConstraint> list3, List<SQLDefaultConstraint> list4,
            List<SQLCheckConstraint> list5)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void dropConstraint(String s, String s1, String s2) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void dropConstraint(String s, String s1, String s2, String s3)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addForeignKey(List<SQLForeignKey> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> list)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> list)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> list)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> list)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        return "";
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String s)
            throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public void dropResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String s, WMNullableResourcePlan wmNullableResourcePlan, boolean b,
            boolean b1, boolean b2) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        return null;
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String s)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropWMTrigger(String s, String s1) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String s)
            throws NoSuchObjectException, MetaException, TException {
        return Lists.newArrayList();
    }

    @Override
    public void createWMPool(WMPool wmPool)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMPool(WMNullablePool wmNullablePool, String s)
            throws NoSuchObjectException, InvalidObjectException, TException {

    }

    @Override
    public void dropWMPool(String s, String s1) throws TException {

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean b) throws TException {

    }

    @Override
    public void dropWMMapping(WMMapping wmMapping) throws TException {

    }

    @Override
    public void createOrDropTriggerToPoolMapping(String s, String s1, String s2, boolean b)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void createISchema(ISchema iSchema) throws TException {

    }

    @Override
    public void alterISchema(String s, String s1, String s2, ISchema iSchema) throws TException {

    }

    @Override
    public ISchema getISchema(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public void dropISchema(String s, String s1, String s2) throws TException {

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {

    }

    @Override
    public SchemaVersion getSchemaVersion(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String s, String s1, String s2) throws TException {
        return Lists.newArrayList();
    }

    @Override
    public void dropSchemaVersion(String s, String s1, String s2, int i) throws TException {

    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
        return null;
    }

    @Override
    public void mapSchemaVersionToSerde(String s, String s1, String s2, int i, String s3) throws TException {

    }

    @Override
    public void setSchemaVersionState(String s, String s1, String s2, int i, SchemaVersionState schemaVersionState)
            throws TException {

    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {

    }

    @Override
    public SerDeInfo getSerDe(String s) throws TException {
        return null;
    }

    @Override
    public LockResponse lockMaterializationRebuild(String s, String s1, long l) throws TException {
        return null;
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String s, String s1, long l) throws TException {
        return false;
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {

    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int i, int i1) throws TException {
        return Lists.newArrayList();
    }
}
