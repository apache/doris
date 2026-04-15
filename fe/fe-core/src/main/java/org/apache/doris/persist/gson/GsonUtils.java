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

package org.apache.doris.persist.gson;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.CloudRollupJobV2;
import org.apache.doris.alter.CloudSchemaChangeJobV2;
import org.apache.doris.alter.RollupJobV2;
import org.apache.doris.alter.SchemaChangeJobV2;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.EncryptKeyRef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IPv4Literal;
import org.apache.doris.analysis.IPv6Literal;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.JsonLiteral;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MapLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.NumericLiteralExpr;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.SearchPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.StructLiteral;
import org.apache.doris.analysis.TimeV2Literal;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.analysis.TryCastExpr;
import org.apache.doris.analysis.VarBinaryLiteral;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.AIResource;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.AnyElementType;
import org.apache.doris.catalog.AnyStructType;
import org.apache.doris.catalog.AnyType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.HMSResource;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.MysqlDBTable;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TemplateType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.load.CloudBrokerLoadJob;
import org.apache.doris.cloud.load.CopyJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergDLFExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergGlueExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergHMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergHadoopExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergJdbcExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergRestExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergS3TablesExternalCatalog;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalCatalog;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalDatabase;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.paimon.PaimonDLFExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonHMSExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonRestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingTaskTxnCommitAttachment;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.BulkLoadJob;
import org.apache.doris.load.loadv2.IngestionLoadJob;
import org.apache.doris.load.loadv2.IngestionLoadJob.IngestionLoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.InsertLoadJob;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.loadv2.MiniLoadTxnCommitAttachment;
import org.apache.doris.load.loadv2.SparkLoadJob;
import org.apache.doris.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadProgress;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.persist.gson.GsonUtilsBase.AtomicBooleanAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.GuavaMultimapAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.GuavaTableAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.HiddenAnnotationExclusionStrategy;
import org.apache.doris.persist.gson.GsonUtilsBase.ImmutableListDeserializer;
import org.apache.doris.persist.gson.GsonUtilsBase.ImmutableMapDeserializer;
import org.apache.doris.persist.gson.GsonUtilsBase.PostProcessTypeAdapterFactory;
import org.apache.doris.persist.gson.GsonUtilsBase.PreProcessTypeAdapterFactory;
import org.apache.doris.persist.gson.GsonUtilsBase.SkipClassExclusionStrategy;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.BrokerHbResponse;
import org.apache.doris.system.FrontendHbResponse;
import org.apache.doris.system.HeartbeatResponse;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ReflectionAccessFilter;
import com.google.gson.ToNumberPolicy;

import java.util.concurrent.atomic.AtomicBoolean;

/*
 * Some utilities about Gson.
 * User should get GSON instance from this class to do the serialization.
 *
 *      GsonUtils.GSON.toJson(...)
 *      GsonUtils.GSON.fromJson(...)
 *
 * More example can be seen in unit test case: "org.apache.doris.common.util.GsonSerializationTest.java".
 *
 * For inherited class serialization, see "org.apache.doris.common.util.GsonDerivedClassSerializationTest.java"
 *
 * And developers may need to add other serialization adapters for custom complex java classes.
 * You need implement a class to implements JsonSerializer and JsonDeserializer, and register it to GSON_BUILDER.
 * See the following "GuavaTableAdapter" and "GuavaMultimapAdapter" for example.
 */
public class GsonUtils {

    // runtime adapter for class "Type"
    private static RuntimeTypeAdapterFactory<org.apache.doris.catalog.Type> columnTypeAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(org.apache.doris.catalog.Type.class, "clazz")
            // TODO: register other sub type after Doris support more types.
            .registerSubtype(ScalarType.class, ScalarType.class.getSimpleName())
            .registerSubtype(ArrayType.class, ArrayType.class.getSimpleName())
            .registerSubtype(MapType.class, MapType.class.getSimpleName())
            .registerSubtype(StructType.class, StructType.class.getSimpleName())
            .registerSubtype(AggStateType.class, AggStateType.class.getSimpleName())
            .registerSubtype(AnyElementType.class, AnyElementType.class.getSimpleName())
            .registerSubtype(AnyStructType.class, AnyStructType.class.getSimpleName())
            .registerSubtype(AnyType.class, AnyType.class.getSimpleName())
            .registerSubtype(TemplateType.class, TemplateType.class.getSimpleName())
            .registerSubtype(VariantType.class, VariantType.class.getSimpleName());

    // runtime adapter for class "Expr"
    private static final RuntimeTypeAdapterFactory<org.apache.doris.analysis.Expr> exprAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(Expr.class, "clazz")
            .registerSubtype(FunctionCallExpr.class, FunctionCallExpr.class.getSimpleName())
            .registerSubtype(LambdaFunctionCallExpr.class, LambdaFunctionCallExpr.class.getSimpleName())
            .registerSubtype(CastExpr.class, CastExpr.class.getSimpleName())
            .registerSubtype(TryCastExpr.class, TryCastExpr.class.getSimpleName())
            .registerSubtype(TimestampArithmeticExpr.class, TimestampArithmeticExpr.class.getSimpleName())
            .registerSubtype(IsNullPredicate.class, IsNullPredicate.class.getSimpleName())
            .registerSubtype(BetweenPredicate.class, BetweenPredicate.class.getSimpleName())
            .registerSubtype(BinaryPredicate.class, BinaryPredicate.class.getSimpleName())
            .registerSubtype(LikePredicate.class, LikePredicate.class.getSimpleName())
            .registerSubtype(MatchPredicate.class, MatchPredicate.class.getSimpleName())
            .registerSubtype(SearchPredicate.class, SearchPredicate.class.getSimpleName())
            .registerSubtype(InPredicate.class, InPredicate.class.getSimpleName())
            .registerSubtype(CompoundPredicate.class, CompoundPredicate.class.getSimpleName())
            .registerSubtype(BoolLiteral.class, BoolLiteral.class.getSimpleName())
            .registerSubtype(MaxLiteral.class, MaxLiteral.class.getSimpleName())
            .registerSubtype(StringLiteral.class, StringLiteral.class.getSimpleName())
            .registerSubtype(IntLiteral.class, IntLiteral.class.getSimpleName())
            .registerSubtype(LargeIntLiteral.class, LargeIntLiteral.class.getSimpleName())
            .registerSubtype(LiteralExpr.class, LiteralExpr.class.getSimpleName())
            .registerSubtype(DecimalLiteral.class, DecimalLiteral.class.getSimpleName())
            .registerSubtype(FloatLiteral.class, FloatLiteral.class.getSimpleName())
            .registerSubtype(NullLiteral.class, NullLiteral.class.getSimpleName())
            .registerSubtype(VarBinaryLiteral.class, VarBinaryLiteral.class.getSimpleName())
            .registerSubtype(MapLiteral.class, MapLiteral.class.getSimpleName())
            .registerSubtype(DateLiteral.class, DateLiteral.class.getSimpleName())
            .registerSubtype(IPv6Literal.class, IPv6Literal.class.getSimpleName())
            .registerSubtype(IPv4Literal.class, IPv4Literal.class.getSimpleName())
            .registerSubtype(TimeV2Literal.class, TimeV2Literal.class.getSimpleName())
            .registerSubtype(JsonLiteral.class, JsonLiteral.class.getSimpleName())
            .registerSubtype(ArrayLiteral.class, ArrayLiteral.class.getSimpleName())
            .registerSubtype(StructLiteral.class, StructLiteral.class.getSimpleName())
            .registerSubtype(NumericLiteralExpr.class, NumericLiteralExpr.class.getSimpleName())
            .registerSubtype(PlaceHolderExpr.class, PlaceHolderExpr.class.getSimpleName())
            .registerSubtype(CaseExpr.class, CaseExpr.class.getSimpleName())
            .registerSubtype(LambdaFunctionExpr.class, LambdaFunctionExpr.class.getSimpleName())
            .registerSubtype(EncryptKeyRef.class, EncryptKeyRef.class.getSimpleName())
            .registerSubtype(ArithmeticExpr.class, ArithmeticExpr.class.getSimpleName())
            .registerSubtype(SlotRef.class, SlotRef.class.getSimpleName())
            .registerSubtype(VirtualSlotRef.class, VirtualSlotRef.class.getSimpleName())
            .registerSubtype(InformationFunction.class, InformationFunction.class.getSimpleName());

    // runtime adapter for class "DistributionInfo"
    private static RuntimeTypeAdapterFactory<DistributionInfo> distributionInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(DistributionInfo.class, "clazz")
            .registerSubtype(HashDistributionInfo.class, HashDistributionInfo.class.getSimpleName())
            .registerSubtype(RandomDistributionInfo.class, RandomDistributionInfo.class.getSimpleName());

    // runtime adapter for class "Resource"
    private static RuntimeTypeAdapterFactory<Resource> resourceTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Resource.class, "clazz")
            .registerSubtype(SparkResource.class, SparkResource.class.getSimpleName())
            .registerSubtype(OdbcCatalogResource.class, OdbcCatalogResource.class.getSimpleName())
            .registerSubtype(S3Resource.class, S3Resource.class.getSimpleName())
            .registerSubtype(JdbcResource.class, JdbcResource.class.getSimpleName())
            .registerSubtype(HdfsResource.class, HdfsResource.class.getSimpleName())
            .registerSubtype(HMSResource.class, HMSResource.class.getSimpleName())
            .registerSubtype(EsResource.class, EsResource.class.getSimpleName())
            .registerSubtype(AIResource.class, AIResource.class.getSimpleName());

    // runtime adapter for class "AlterJobV2"
    private static RuntimeTypeAdapterFactory<AlterJobV2> alterJobV2TypeAdapterFactory;

    static {
        alterJobV2TypeAdapterFactory = RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                .registerSubtype(CloudSchemaChangeJobV2.class, CloudSchemaChangeJobV2.class.getSimpleName())
                .registerSubtype(CloudRollupJobV2.class, CloudRollupJobV2.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            alterJobV2TypeAdapterFactory
                    .registerSubtype(RollupJobV2.class, RollupJobV2.class.getSimpleName())
                    .registerSubtype(SchemaChangeJobV2.class, SchemaChangeJobV2.class.getSimpleName());
        } else {
            // compatible with old cloud code.
            alterJobV2TypeAdapterFactory
                    .registerCompatibleSubtype(CloudRollupJobV2.class, RollupJobV2.class.getSimpleName())
                    .registerCompatibleSubtype(CloudSchemaChangeJobV2.class, SchemaChangeJobV2.class.getSimpleName());
        }
    }

    // runtime adapter for class "LoadJobStateUpdateInfo"
    private static RuntimeTypeAdapterFactory<LoadJobStateUpdateInfo> loadJobStateUpdateInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(LoadJobStateUpdateInfo.class, "clazz")
            .registerSubtype(SparkLoadJobStateUpdateInfo.class, SparkLoadJobStateUpdateInfo.class.getSimpleName())
            .registerSubtype(IngestionLoadJobStateUpdateInfo.class,
                    IngestionLoadJobStateUpdateInfo.class.getSimpleName());

    // runtime adapter for class "Policy"
    private static RuntimeTypeAdapterFactory<Policy> policyTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    Policy.class, "clazz").registerSubtype(RowPolicy.class, RowPolicy.class.getSimpleName())
            .registerSubtype(StoragePolicy.class, StoragePolicy.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<Constraint> constraintTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    Constraint.class, "clazz")
            .registerSubtype(PrimaryKeyConstraint.class, PrimaryKeyConstraint.class.getSimpleName())
            .registerSubtype(ForeignKeyConstraint.class, ForeignKeyConstraint.class.getSimpleName())
            .registerSubtype(UniqueConstraint.class, UniqueConstraint.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<CatalogIf> dsTypeAdapterFactory;

    static {
        dsTypeAdapterFactory = RuntimeTypeAdapterFactory.of(CatalogIf.class, "clazz")
                .registerSubtype(CloudInternalCatalog.class, CloudInternalCatalog.class.getSimpleName())
                .registerSubtype(HMSExternalCatalog.class, HMSExternalCatalog.class.getSimpleName())
                .registerSubtype(EsExternalCatalog.class, EsExternalCatalog.class.getSimpleName())
                .registerSubtype(JdbcExternalCatalog.class, JdbcExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergExternalCatalog.class, IcebergExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergHMSExternalCatalog.class, IcebergHMSExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergGlueExternalCatalog.class, IcebergGlueExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergRestExternalCatalog.class, IcebergRestExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergDLFExternalCatalog.class, IcebergDLFExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergHadoopExternalCatalog.class, IcebergHadoopExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergJdbcExternalCatalog.class, IcebergJdbcExternalCatalog.class.getSimpleName())
                .registerSubtype(IcebergS3TablesExternalCatalog.class,
                        IcebergS3TablesExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonExternalCatalog.class, PaimonExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonHMSExternalCatalog.class, PaimonHMSExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonFileExternalCatalog.class, PaimonFileExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonRestExternalCatalog.class, PaimonRestExternalCatalog.class.getSimpleName())
                .registerSubtype(MaxComputeExternalCatalog.class, MaxComputeExternalCatalog.class.getSimpleName())
                .registerSubtype(
                            TrinoConnectorExternalCatalog.class, TrinoConnectorExternalCatalog.class.getSimpleName())
                .registerSubtype(LakeSoulExternalCatalog.class, LakeSoulExternalCatalog.class.getSimpleName())
                .registerSubtype(TestExternalCatalog.class, TestExternalCatalog.class.getSimpleName())
                .registerSubtype(PaimonDLFExternalCatalog.class, PaimonDLFExternalCatalog.class.getSimpleName())
                .registerSubtype(RemoteDorisExternalCatalog.class, RemoteDorisExternalCatalog.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            dsTypeAdapterFactory
                    .registerSubtype(InternalCatalog.class, InternalCatalog.class.getSimpleName());
        } else {
            // compatible with old cloud code.
            dsTypeAdapterFactory
                    .registerCompatibleSubtype(CloudInternalCatalog.class, InternalCatalog.class.getSimpleName());
        }
    }

    // routine load data source
    private static RuntimeTypeAdapterFactory<AbstractDataSourceProperties> rdsTypeAdapterFactory =
            RuntimeTypeAdapterFactory.of(
                            AbstractDataSourceProperties.class, "clazz")
                    .registerSubtype(KafkaDataSourceProperties.class, KafkaDataSourceProperties.class.getSimpleName());
    private static RuntimeTypeAdapterFactory<org.apache.doris.job.base.AbstractJob>
            jobExecutorRuntimeTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(org.apache.doris.job.base.AbstractJob.class, "clazz")
                            .registerSubtype(InsertJob.class, InsertJob.class.getSimpleName())
                            .registerSubtype(MTMVJob.class, MTMVJob.class.getSimpleName())
                            .registerSubtype(StreamingInsertJob.class, StreamingInsertJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<MTMVSnapshotIf> mtmvSnapshotTypeAdapterFactory =
            RuntimeTypeAdapterFactory.of(MTMVSnapshotIf.class, "clazz")
                    .registerSubtype(MTMVMaxTimestampSnapshot.class, MTMVMaxTimestampSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVTimestampSnapshot.class, MTMVTimestampSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVSnapshotIdSnapshot.class, MTMVSnapshotIdSnapshot.class.getSimpleName())
                    .registerSubtype(MTMVVersionSnapshot.class, MTMVVersionSnapshot.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<DatabaseIf> dbTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    DatabaseIf.class, "clazz")
            .registerSubtype(ExternalDatabase.class, ExternalDatabase.class.getSimpleName())
            .registerSubtype(EsExternalDatabase.class, EsExternalDatabase.class.getSimpleName())
            .registerSubtype(HMSExternalDatabase.class, HMSExternalDatabase.class.getSimpleName())
            .registerSubtype(JdbcExternalDatabase.class, JdbcExternalDatabase.class.getSimpleName())
            .registerSubtype(IcebergExternalDatabase.class, IcebergExternalDatabase.class.getSimpleName())
            .registerSubtype(LakeSoulExternalDatabase.class, LakeSoulExternalDatabase.class.getSimpleName())
            .registerSubtype(PaimonExternalDatabase.class, PaimonExternalDatabase.class.getSimpleName())
            .registerSubtype(MaxComputeExternalDatabase.class, MaxComputeExternalDatabase.class.getSimpleName())
            .registerSubtype(ExternalInfoSchemaDatabase.class, ExternalInfoSchemaDatabase.class.getSimpleName())
            .registerSubtype(ExternalMysqlDatabase.class, ExternalMysqlDatabase.class.getSimpleName())
            .registerSubtype(TrinoConnectorExternalDatabase.class, TrinoConnectorExternalDatabase.class.getSimpleName())
            .registerSubtype(TestExternalDatabase.class, TestExternalDatabase.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<TableIf> tblTypeAdapterFactory = RuntimeTypeAdapterFactory.of(
                    TableIf.class, "clazz").registerSubtype(ExternalTable.class, ExternalTable.class.getSimpleName())
            .registerSubtype(EsExternalTable.class, EsExternalTable.class.getSimpleName())
            .registerSubtype(OlapTable.class, OlapTable.class.getSimpleName())
            .registerSubtype(HMSExternalTable.class, HMSExternalTable.class.getSimpleName())
            .registerSubtype(JdbcExternalTable.class, JdbcExternalTable.class.getSimpleName())
            .registerSubtype(IcebergExternalTable.class, IcebergExternalTable.class.getSimpleName())
            .registerSubtype(LakeSoulExternalTable.class, LakeSoulExternalTable.class.getSimpleName())
            .registerSubtype(PaimonExternalTable.class, PaimonExternalTable.class.getSimpleName())
            .registerSubtype(MaxComputeExternalTable.class, MaxComputeExternalTable.class.getSimpleName())
            .registerSubtype(ExternalInfoSchemaTable.class, ExternalInfoSchemaTable.class.getSimpleName())
            .registerSubtype(ExternalMysqlTable.class, ExternalMysqlTable.class.getSimpleName())
            .registerSubtype(TrinoConnectorExternalTable.class, TrinoConnectorExternalTable.class.getSimpleName())
            .registerSubtype(TestExternalTable.class, TestExternalTable.class.getSimpleName())
            .registerSubtype(BrokerTable.class, BrokerTable.class.getSimpleName())
            .registerSubtype(EsTable.class, EsTable.class.getSimpleName())
            .registerSubtype(FunctionGenTable.class, FunctionGenTable.class.getSimpleName())
            .registerSubtype(HiveTable.class, HiveTable.class.getSimpleName())
            .registerSubtype(InlineView.class, InlineView.class.getSimpleName())
            .registerSubtype(JdbcTable.class, JdbcTable.class.getSimpleName())
            .registerSubtype(MTMV.class, MTMV.class.getSimpleName())
            .registerSubtype(MysqlDBTable.class, MysqlDBTable.class.getSimpleName())
            .registerSubtype(MysqlTable.class, MysqlTable.class.getSimpleName())
            .registerSubtype(OdbcTable.class, OdbcTable.class.getSimpleName())
            .registerSubtype(SchemaTable.class, SchemaTable.class.getSimpleName())
            .registerSubtype(View.class, View.class.getSimpleName())
            .registerSubtype(Dictionary.class, Dictionary.class.getSimpleName())
            .registerSubtype(OlapTableStream.class, OlapTableStream.class.getSimpleName());

    // runtime adapter for class "PartitionInfo"
    private static RuntimeTypeAdapterFactory<PartitionInfo> partitionInfoTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(PartitionInfo.class, "clazz")
            .registerSubtype(ListPartitionInfo.class, ListPartitionInfo.class.getSimpleName())
            .registerSubtype(RangePartitionInfo.class, RangePartitionInfo.class.getSimpleName())
            .registerSubtype(SinglePartitionInfo.class, SinglePartitionInfo.class.getSimpleName());

    // runtime adapter for class "HeartbeatResponse"
    private static RuntimeTypeAdapterFactory<HeartbeatResponse> hbResponseTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(HeartbeatResponse.class, "clazz")
            .registerSubtype(BackendHbResponse.class, BackendHbResponse.class.getSimpleName())
            .registerSubtype(FrontendHbResponse.class, FrontendHbResponse.class.getSimpleName())
            .registerSubtype(BrokerHbResponse.class, BrokerHbResponse.class.getSimpleName());

    // runtime adapter for class "Function"
    private static RuntimeTypeAdapterFactory<Function> functionAdapterFactory
            = RuntimeTypeAdapterFactory.of(Function.class, "clazz")
            .registerSubtype(ScalarFunction.class, ScalarFunction.class.getSimpleName())
            .registerSubtype(AggregateFunction.class, AggregateFunction.class.getSimpleName())
            .registerSubtype(AliasFunction.class, AliasFunction.class.getSimpleName());

    // runtime adapter for class "CloudReplica".
    private static RuntimeTypeAdapterFactory<Replica> replicaTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Replica.class, "clazz")
            .registerSubtype(LocalReplica.class, LocalReplica.class.getSimpleName())
            .registerSubtype(CloudReplica.class, CloudReplica.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<Tablet> tabletTypeAdapterFactory;

    static {
        tabletTypeAdapterFactory = RuntimeTypeAdapterFactory
                .of(Tablet.class, "clazz")
                .registerSubtype(LocalTablet.class, LocalTablet.class.getSimpleName())
                .registerSubtype(CloudTablet.class, CloudTablet.class.getSimpleName());
        if (Config.isNotCloudMode()) {
            tabletTypeAdapterFactory.registerDefaultSubtype(LocalTablet.class);
            tabletTypeAdapterFactory.registerCompatibleSubtype(LocalTablet.class, Tablet.class.getSimpleName());
            replicaTypeAdapterFactory.registerDefaultSubtype(LocalReplica.class);
            replicaTypeAdapterFactory.registerCompatibleSubtype(LocalReplica.class, Replica.class.getSimpleName());
        } else {
            // compatible with old cloud code.
            tabletTypeAdapterFactory.registerDefaultSubtype(CloudTablet.class);
            replicaTypeAdapterFactory.registerDefaultSubtype(CloudReplica.class);
        }
    }

    // runtime adapter for class "CloudPartition".
    private static RuntimeTypeAdapterFactory<Partition> partitionTypeAdapterFactory = RuntimeTypeAdapterFactory
            .of(Partition.class, "clazz")
            .registerDefaultSubtype(Partition.class)
            .registerSubtype(Partition.class, Partition.class.getSimpleName())
            .registerSubtype(CloudPartition.class, CloudPartition.class.getSimpleName());

    // runtime adapter for class "TxnCommitAttachment".
    private static RuntimeTypeAdapterFactory<TxnCommitAttachment> txnCommitAttachmentTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(TxnCommitAttachment.class, "clazz")
            .registerDefaultSubtype(TxnCommitAttachment.class)
            .registerSubtype(LoadJobFinalOperation.class, LoadJobFinalOperation.class.getSimpleName())
            .registerSubtype(MiniLoadTxnCommitAttachment.class, MiniLoadTxnCommitAttachment.class.getSimpleName())
            .registerSubtype(RLTaskTxnCommitAttachment.class, RLTaskTxnCommitAttachment.class.getSimpleName())
            .registerSubtype(StreamingTaskTxnCommitAttachment.class,
                    StreamingTaskTxnCommitAttachment.class.getSimpleName());

    // runtime adapter for class "RoutineLoadProgress".
    private static RuntimeTypeAdapterFactory<RoutineLoadProgress> routineLoadTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(RoutineLoadProgress.class, "clazz")
            .registerDefaultSubtype(RoutineLoadProgress.class)
            .registerSubtype(KafkaProgress.class, KafkaProgress.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<RoutineLoadJob> routineLoadJobTypeAdapterFactory
            = RuntimeTypeAdapterFactory.of(RoutineLoadJob.class, "clazz")
            .registerDefaultSubtype(RoutineLoadJob.class)
            .registerSubtype(KafkaRoutineLoadJob.class, KafkaRoutineLoadJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<org.apache.doris.backup.AbstractJob>
            jobBackupTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(org.apache.doris.backup.AbstractJob.class, "clazz")
                    .registerSubtype(BackupJob.class, BackupJob.class.getSimpleName())
                    .registerSubtype(RestoreJob.class, RestoreJob.class.getSimpleName())
                    .registerSubtype(CloudRestoreJob.class, CloudRestoreJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<LoadJob> loadJobTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(LoadJob.class, "clazz")
                    .registerSubtype(BrokerLoadJob.class, BrokerLoadJob.class.getSimpleName())
                    .registerSubtype(BulkLoadJob.class, BulkLoadJob.class.getSimpleName())
                    .registerSubtype(CloudBrokerLoadJob.class, CloudBrokerLoadJob.class.getSimpleName())
                    .registerSubtype(CopyJob.class, CopyJob.class.getSimpleName())
                    .registerSubtype(InsertLoadJob.class, InsertLoadJob.class.getSimpleName())
                    .registerSubtype(SparkLoadJob.class, SparkLoadJob.class.getSimpleName())
                    .registerSubtype(IngestionLoadJob.class, IngestionLoadJob.class.getSimpleName());

    private static RuntimeTypeAdapterFactory<PartitionItem> partitionItemTypeAdapterFactory
                    = RuntimeTypeAdapterFactory.of(PartitionItem.class, "clazz")
                    .registerSubtype(ListPartitionItem.class, ListPartitionItem.class.getSimpleName())
                    .registerSubtype(RangePartitionItem.class, RangePartitionItem.class.getSimpleName());

    // the builder of GSON instance.
    // Add any other adapters if necessary.
    //
    // ATTN:
    // Since GsonBuilder.create() adds all registered factories to GSON in reverse order, if you
    // need to ensure the search order of two RuntimeTypeAdapterFactory instances, be sure to
    // register them in reverse priority order.
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .setExclusionStrategies(new SkipClassExclusionStrategy())
            .addSerializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization()
            .addReflectionAccessFilter(ReflectionAccessFilter.BLOCK_INACCESSIBLE_JAVA)
            .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter<>())
            .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter<>())
            .registerTypeAdapterFactory(new PostProcessTypeAdapterFactory())
            .registerTypeAdapterFactory(new PreProcessTypeAdapterFactory())
            .registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer())
            .registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer())
            .registerTypeAdapter(AtomicBoolean.class, new AtomicBooleanAdapter())
            .registerTypeAdapterFactory(exprAdapterFactory)
            .registerTypeAdapterFactory(columnTypeAdapterFactory)
            .registerTypeAdapterFactory(distributionInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(resourceTypeAdapterFactory)
            .registerTypeAdapterFactory(alterJobV2TypeAdapterFactory)
            .registerTypeAdapterFactory(loadJobStateUpdateInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(policyTypeAdapterFactory).registerTypeAdapterFactory(dsTypeAdapterFactory)
            .registerTypeAdapterFactory(dbTypeAdapterFactory).registerTypeAdapterFactory(tblTypeAdapterFactory)
            .registerTypeAdapterFactory(replicaTypeAdapterFactory)
            .registerTypeAdapterFactory(tabletTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionInfoTypeAdapterFactory)
            .registerTypeAdapterFactory(hbResponseTypeAdapterFactory)
            .registerTypeAdapterFactory(functionAdapterFactory)
            .registerTypeAdapterFactory(rdsTypeAdapterFactory)
            .registerTypeAdapterFactory(jobExecutorRuntimeTypeAdapterFactory)
            .registerTypeAdapterFactory(mtmvSnapshotTypeAdapterFactory)
            .registerTypeAdapterFactory(constraintTypeAdapterFactory)
            .registerTypeAdapterFactory(txnCommitAttachmentTypeAdapterFactory)
            .registerTypeAdapterFactory(routineLoadTypeAdapterFactory)
            .registerTypeAdapterFactory(routineLoadJobTypeAdapterFactory)
            .registerTypeAdapterFactory(jobBackupTypeAdapterFactory)
            .registerTypeAdapterFactory(loadJobTypeAdapterFactory)
            .registerTypeAdapterFactory(partitionItemTypeAdapterFactory)
            .registerTypeAdapter(PartitionKey.class, new PartitionKey.PartitionKeySerializer())
            .registerTypeAdapter(Range.class, new RangeUtils.RangeSerializer());


    // this instance is thread-safe.
    public static final Gson GSON = GSON_BUILDER.create();

    // ATTN: the order between creating GSON and GSON_PRETTY_PRINTING is very important.
    private static final GsonBuilder GSON_BUILDER_PRETTY_PRINTING = GSON_BUILDER.setPrettyPrinting();
    public static final Gson GSON_PRETTY_PRINTING = GSON_BUILDER_PRETTY_PRINTING.create();
}
