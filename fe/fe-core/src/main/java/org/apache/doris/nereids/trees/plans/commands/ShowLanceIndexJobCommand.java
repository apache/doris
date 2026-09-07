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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobResult;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SHOW LANCE INDEX JOB &lt;jobId&gt;.
 *
 * <p>Non-disclosing job detail (v5.1 section 8): the record is loaded first and authorized
 * against its persisted target before any field is returned. Orphan and half-orphan targets
 * (catalog gone, or persisted db/table no longer resolvable) require global ADMIN; a
 * resolvable target requires table-level SHOW. A missing job and an unauthorized job share
 * the same fixed ERR_LANCE_INDEX_JOB_NOT_FOUND response that names only the job id, so the
 * existence and target of a job are never disclosed. The job locator, provider, normalized
 * names, propertiesJson and schema contract contents are never shown.
 */
public class ShowLanceIndexJobCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .addAll(ShowLanceIndexJobsCommand.TITLE_NAMES)
            .add("Creator")
            .add("ResultCode")
            .add("CompletionReason")
            .add("ExternalMetadataAdvanced")
            .add("BackendId")
            .add("BeProcessEpoch")
            .add("InvocationId")
            .add("DeadlineMs")
            .add("TerminationProof")
            .add("AdmittedDatasetVersion")
            .add("SchemaContractVersion")
            .add("Revision")
            .add("ForceNote")
            .add("ForceWarning")
            .add("IfNotExists")
            .add("IfExists")
            .add("IndexType")
            .add("ColumnName")
            .build();

    private final long jobId;

    public ShowLanceIndexJobCommand(long jobId) {
        super(PlanType.SHOW_LANCE_INDEX_JOB_COMMAND);
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LanceIndexJob job = Env.getCurrentEnv().getLanceIndexJobManager().getJob(jobId);
        if (job == null) {
            throw notFound();
        }
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog =
                Env.getCurrentEnv().getCatalogMgr().getCatalog(job.getCatalogId());
        // Missing and unauthorized jobs share the same fixed response; authorize before
        // returning any field.
        if (!ShowLanceIndexJobsCommand.isAuthorized(ctx, catalog, job)) {
            throw notFound();
        }
        return new ShowResultSet(getMetaData(), Collections.singletonList(renderRow(job, catalog)));
    }

    private AnalysisException notFound() {
        return new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND.formatErrorMsg(jobId),
                ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND);
    }

    private static List<String> renderRow(LanceIndexJob job,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
        List<String> row = new ArrayList<>(TITLE_NAMES.size());
        row.add(String.valueOf(job.getJobId()));
        // See ShowLanceIndexJobsCommand: the catalog name is not persisted on the job record,
        // so an orphan row renders it empty rather than fabricating one from the internal id.
        row.add(catalog == null ? "" : catalog.getName());
        row.add(job.getDbName());
        row.add(job.getTableName());
        row.add(job.getDisplayIndexName());
        row.add(job.getMutationType() == null ? "" : job.getMutationType().name());
        row.add(job.getMutationState() == null ? "" : job.getMutationState().name());
        row.add(job.getRefreshState() == null ? "" : job.getRefreshState().name());
        row.add(job.holdsPossibleLiveSlot() ? "YES" : "NO");
        row.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
        row.add(TimeUtils.longToTimeString(job.getUpdateTimeMs()));
        LanceIndexJobResult result = job.getResult();
        // The result is always null until a worker reports (no worker exists yet); the same
        // holds for every dispatch field. All of them render as empty strings, never NPE.
        row.add(result == null || result.getSanitizedMessage() == null ? "" : result.getSanitizedMessage());
        row.addAll(ShowLanceIndexJobsCommand.renderForceAudit(job));
        row.add(job.getCreator() == null ? "" : job.getCreator());
        row.add(result == null ? "" : result.getResultCode().name());
        row.add(result == null || result.getCompletionReason() == null ? "" : result.getCompletionReason().name());
        row.add(result == null ? "" : (result.isExternalMetadataAdvanced() ? "YES" : "NO"));
        row.add(job.getBackendId() == null ? "" : String.valueOf(job.getBackendId()));
        row.add(job.getBeProcessEpoch() == null ? "" : String.valueOf(job.getBeProcessEpoch()));
        row.add(job.getInvocationId() == null ? "" : job.getInvocationId());
        row.add(job.getDeadlineMs() == null ? "" : String.valueOf(job.getDeadlineMs()));
        row.add(job.getTerminationProof() == null ? "" : job.getTerminationProof().name());
        row.add(String.valueOf(job.getAdmittedDatasetVersion()));
        row.add(job.getSchemaContract() == null
                ? "" : String.valueOf(job.getSchemaContract().getSchemaContractVersion()));
        row.add(String.valueOf(job.getRevision()));
        row.add(job.isForceReleased() && job.getForceNote() != null ? job.getForceNote() : "");
        row.add(job.isForceReleased() && job.getForceWarning() != null ? job.getForceWarning() : "");
        row.add(job.isIfNotExists() ? "YES" : "NO");
        row.add(job.isIfExists() ? "YES" : "NO");
        row.add(job.getIndexType() == null ? "" : job.getIndexType());
        row.add(job.getColumnName() == null ? "" : job.getColumnName());
        return row;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLanceIndexJobCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
