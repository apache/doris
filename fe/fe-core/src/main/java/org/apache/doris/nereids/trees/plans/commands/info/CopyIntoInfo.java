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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CopyFromParam;
import org.apache.doris.analysis.CopyIntoProperties;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StageAndPattern;
import org.apache.doris.analysis.StageProperties;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.constants.BosProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * copy into informations
 */
public class CopyIntoInfo {
    private static final Logger LOG = LogManager.getLogger(CopyIntoInfo.class);

    private static final ShowResultSetMetaData COPY_INTO_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("id", ScalarType.createVarchar(64)))
                .addColumn(new Column("state", ScalarType.createVarchar(64)))
                .addColumn(new Column("type", ScalarType.createVarchar(64)))
                .addColumn(new Column("msg", ScalarType.createVarchar(128)))
                .addColumn(new Column("loadedRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("filterRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("unselectRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("url", ScalarType.createVarchar(128)))
                .build();
    private static final String S3_BUCKET = "bucket";
    private static final String S3_PREFIX = "prefix";

    private final List<String> nameParts;
    private CopyFromDesc copyFromDesc;
    private CopyFromParam legacyCopyFromParam;
    private CopyIntoProperties copyIntoProperties;
    private Map<String, Map<String, String>> optHints;

    private LabelName label = null;
    private BrokerDesc brokerDesc = null;
    private DataDescription dataDescription = null;
    private final Map<String, String> brokerProperties = new HashMap<>();
    private Map<String, String> properties = new HashMap<>();

    private String stage;
    private String stageId;
    private StageType stageType;
    private String stagePrefix;
    private RemoteBase.ObjectInfo objectInfo;
    private String userName;
    private TableName tableName;

    private OriginStatement originStmt;

    /**
     * copy into informations
     */
    public CopyIntoInfo(List<String> nameParts, CopyFromDesc copyFromDesc,
                        Map<String, String> properties, Map<String, Map<String, String>> optHints) {
        this.nameParts = nameParts;
        this.copyFromDesc = copyFromDesc;
        Map<String, String> newProperties = new HashMap<>();
        for (String key : properties.keySet()) {
            newProperties.put(key, properties.get(key));
        }
        this.copyIntoProperties = new CopyIntoProperties(newProperties);
        this.optHints = optHints;
        this.stage = copyFromDesc.getStageAndPattern().getStageName();
    }

    /**
     * validate copy into information
     */
    public void validate(ConnectContext ctx) throws DdlException, AnalysisException {
        if (this.optHints != null && this.optHints.containsKey(SessionVariable.CLOUD_CLUSTER)) {
            ((CloudEnv) Env.getCurrentEnv())
                    .checkCloudClusterPriv(this.optHints.get("set_var").get(SessionVariable.CLOUD_CLUSTER));
        }
        // generate a label
        String labelName = "copy_" + DebugUtil.printId(ctx.queryId()).replace("-", "_");
        String ctl = null;
        String db = null;
        String table = null;
        switch (nameParts.size()) {
            case 1: { // table
                ctl = ctx.getDefaultCatalog();
                if (Strings.isNullOrEmpty(ctl)) {
                    ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
                }
                db = ctx.getDatabase();
                if (Strings.isNullOrEmpty(db)) {
                    throw new AnalysisException("Please specify a database name.");
                }
                table = nameParts.get(0);
                break;
            }
            case 2:
                // db.table
                // Use database name from table name parts.
                break;
            case 3: {
                // catalog.db.table
                ctl = nameParts.get(0);
                db = nameParts.get(1);
                table = nameParts.get(2);
                break;
            }
            default:
                throw new IllegalStateException("Table name [" + nameParts + "] is invalid.");
        }
        tableName = new TableName(ctl, db, table);
        label = new LabelName(tableName.getDb(), labelName);
        if (stage.isEmpty()) {
            throw new AnalysisException("Stage name can not be empty");
        }
        this.userName = ClusterNamespace.getNameFromFullName(ctx.getCurrentUserIdentity().getQualifiedUser());
        doValidate(userName, db, true);
    }

    /**
     * do validate
     */
    public void doValidate(String user, String db, boolean checkAuth) throws AnalysisException, DdlException {
        // get stage from meta service
        StagePB stagePB = StageUtil.getStage(stage, userName, true);
        validateStagePB(stagePB);
        // generate broker desc
        brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerProperties);
        // generate data description
        String filePath = "s3://" + brokerProperties.get(S3_BUCKET) + "/" + brokerProperties.get(S3_PREFIX);
        Separator separator = copyIntoProperties.getColumnSeparator() != null ? new Separator(
                copyIntoProperties.getColumnSeparator()) : null;
        String fileFormatStr = copyIntoProperties.getFileType();
        Map<String, String> dataDescProperties = copyIntoProperties.getDataDescriptionProperties();
        copyFromDesc.validate(db, tableName, this.copyIntoProperties.useDeleteSign(),
                copyIntoProperties.getFileTypeIgnoreCompression());
        if (LOG.isDebugEnabled()) {
            LOG.debug("copy into params. sql: {}, fileColumns: {}, columnMappingList: {}, filter: {}",
                    copyFromDesc.getFileColumns().toString(), copyFromDesc.getColumnMappingList().toString(),
                    copyFromDesc.getFileFilterExpr().toString());
        }

        List<String> nameParts = Lists.newArrayList();
        nameParts.add(db);
        nameParts.add(tableName.getTbl());
        Plan unboundRelation = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), nameParts);
        CascadesContext cascadesContext = CascadesContext.initContext(ConnectContext.get().getStatementContext(),
                unboundRelation, PhysicalProperties.ANY);
        Analyzer.buildCustomAnalyzer(
                cascadesContext, ImmutableList.of(Analyzer.bottomUp(new BindRelation()))
        ).execute();
        Rewriter.getWholeTreeRewriterWithCustomJobs(
                cascadesContext, ImmutableList.of()
        ).execute();
        Plan boundRelation = cascadesContext.getRewritePlan();
        // table could have delete sign in LogicalFilter above
        if (cascadesContext.getRewritePlan() instanceof LogicalFilter) {
            boundRelation = (Plan) ((LogicalFilter) cascadesContext.getRewritePlan()).child();
        }
        PlanTranslatorContext context = new PlanTranslatorContext(cascadesContext);
        List<Slot> slots = boundRelation.getOutput();
        Scope scope = new Scope(slots);
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, scope, cascadesContext, false, false);

        Map<SlotReference, SlotRef> translateMap = Maps.newHashMap();

        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(((OlapScan) boundRelation).getTable());
        for (int i = 0; i < boundRelation.getOutput().size(); i++) {
            SlotReference slotReference = (SlotReference) boundRelation.getOutput().get(i);
            SlotRef slotRef = new SlotRef(null, slotReference.getName());
            translateMap.put(slotReference, slotRef);
            context.createSlotDesc(tupleDescriptor, slotReference, ((OlapScan) boundRelation).getTable());
        }

        List<Expr> legacyColumnMappingList = null;
        if (copyFromDesc.getColumnMappingList() != null && !copyFromDesc.getColumnMappingList().isEmpty()) {
            legacyColumnMappingList = new ArrayList<>();
            for (Expression expression : copyFromDesc.getColumnMappingList()) {
                legacyColumnMappingList.add(translateToLegacyExpr(expression, analyzer, context, cascadesContext));
            }
        }
        Expr legacyFileFilterExpr = null;
        if (copyFromDesc.getFileFilterExpr().isPresent()) {
            legacyFileFilterExpr = translateToLegacyExpr(copyFromDesc.getFileFilterExpr().get(),
                    analyzer, context, cascadesContext);
        }

        dataDescProperties.put(FileFormatProperties.PROP_COMPRESS_TYPE, copyIntoProperties.getCompression());
        dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath),
            copyFromDesc.getFileColumns(), separator, fileFormatStr, null, false,
            legacyColumnMappingList, legacyFileFilterExpr, null, LoadTask.MergeType.APPEND, null,
            null, dataDescProperties);
        if (!(copyFromDesc.getColumnMappingList() == null
                || copyFromDesc.getColumnMappingList().isEmpty())) {
            dataDescription.setIgnoreCsvRedundantCol(true);
        }
        // analyze data description
        if (checkAuth) {
            dataDescription.analyze(db);
        } else {
            dataDescription.analyzeWithoutCheckPriv(db);
        }
        String path;
        for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
            path = dataDescription.getFilePaths().get(i);
            dataDescription.getFilePaths().set(i, BosProperties.convertPathToS3(path));
            StorageBackend.checkPath(path, brokerDesc.getStorageType(), null);
            dataDescription.getFilePaths().set(i, path);
        }

        try {
            properties.putAll(copyIntoProperties.getExecProperties());
            // TODO support exec params as LoadStmt
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

        // translate copy from description to copy from param
        legacyCopyFromParam = toLegacyParam(copyFromDesc, analyzer, context, cascadesContext);
    }

    private CopyFromParam toLegacyParam(CopyFromDesc copyFromDesc, ExpressionAnalyzer analyzer,
                                        PlanTranslatorContext context, CascadesContext cascadesContext) {
        StageAndPattern stageAndPattern = copyFromDesc.getStageAndPattern();
        List<Expr> exprList = null;
        if (copyFromDesc.getExprList() != null) {
            exprList = new ArrayList<>();
            for (Expression expression : copyFromDesc.getExprList()) {
                exprList.add(translateToLegacyExpr(expression, analyzer, context, cascadesContext));
            }
        }
        Expr fileFilterExpr = null;
        if (copyFromDesc.getFileFilterExpr().isPresent()) {
            fileFilterExpr = translateToLegacyExpr(copyFromDesc.getFileFilterExpr().get(),
                    analyzer, context, cascadesContext);
        }
        List<String> fileColumns = copyFromDesc.getFileColumns();
        List<Expr> columnMappingList = null;
        if (copyFromDesc.getColumnMappingList() != null) {
            columnMappingList = new ArrayList<>();
            for (Expression expression : copyFromDesc.getColumnMappingList()) {
                columnMappingList.add(translateToLegacyExpr(expression, analyzer, context, cascadesContext));
            }
        }
        List<String> targetColumns = copyFromDesc.getTargetColumns();
        return new CopyFromParam(stageAndPattern, exprList, fileFilterExpr, fileColumns, columnMappingList,
                targetColumns);
    }

    private Expr translateToLegacyExpr(Expression expr, ExpressionAnalyzer analyzer, PlanTranslatorContext context,
                                       CascadesContext cascadesContext) {
        Expression expression;
        try {
            expression = analyzer.analyze(expr, new ExpressionRewriteContext(cascadesContext));
        } catch (org.apache.doris.nereids.exceptions.AnalysisException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("In where clause '"
                + expr.toSql() + "', "
                + Utils.convertFirstChar(e.getMessage()));
        }
        ExpressionToExpr translator = new ExpressionToExpr();
        return expression.accept(translator, context);
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitCast(Cast cast, PlanTranslatorContext context) {
            // left child of cast is target type, right child of cast is expression
            return new CastExpr(cast.getDataType().toCatalogDataType(),
                    cast.child().accept(this, context), null);
        }
    }

    // after validateStagePB, fileFormat and copyOption is not null
    private void validateStagePB(StagePB stagePB) throws AnalysisException {
        stageType = stagePB.getType();
        stageId = stagePB.getStageId();
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        stagePrefix = objInfo.getPrefix();
        objectInfo = RemoteBase.analyzeStageObjectStoreInfo(stagePB);
        brokerProperties.put(S3Properties.Env.ENDPOINT, objInfo.getEndpoint());
        brokerProperties.put(S3Properties.Env.REGION, objInfo.getRegion());
        brokerProperties.put(S3Properties.Env.ACCESS_KEY, objectInfo.getAk());
        brokerProperties.put(S3Properties.Env.SECRET_KEY, objectInfo.getSk());
        if (objectInfo.getToken() != null) {
            brokerProperties.put(S3Properties.Env.TOKEN, objectInfo.getToken());
        }
        brokerProperties.put(S3_BUCKET, objInfo.getBucket());
        brokerProperties.put(S3_PREFIX, objInfo.getPrefix());
        // S3 Provider properties should be case insensitive.
        brokerProperties.put(S3Properties.PROVIDER, objInfo.getProvider().toString().toUpperCase());
        StageProperties stageProperties = new StageProperties(stagePB.getPropertiesMap());
        this.copyIntoProperties.mergeProperties(stageProperties);
        this.copyIntoProperties.analyze();
    }

    public ShowResultSetMetaData getMetaData() {
        return COPY_INTO_META_DATA;
    }

    public String getDbName() {
        return label.getDbName();
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public List<DataDescription> getDataDescriptions() {
        return Lists.newArrayList(dataDescription);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public LabelName getLabel() {
        return label;
    }

    public OriginStatement getOriginStmt() {
        return originStmt;
    }

    public void setOriginStmt(OriginStatement originStmt) {
        this.originStmt = originStmt;
    }

    public String getStage() {
        return stage;
    }

    public String getStageId() {
        return stageId;
    }

    public StageType getStageType() {
        return stageType;
    }

    public String getStagePrefix() {
        return stagePrefix;
    }

    public long getSizeLimit() {
        return this.copyIntoProperties.getSizeLimit();
    }

    public boolean isAsync() {
        return this.copyIntoProperties.isAsync();
    }

    public boolean isForce() {
        return this.copyIntoProperties.isForce();
    }

    public String getUserName() {
        return userName;
    }

    public RemoteBase.ObjectInfo getObjectInfo() {
        return objectInfo;
    }

    public String getPattern() {
        return this.legacyCopyFromParam.getStageAndPattern().getPattern();
    }
}
