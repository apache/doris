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

package org.apache.doris.load.loadv2;

import com.google.common.base.Strings;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * There are 4 steps in SparkLoadJob:
 * Step1: SparkLoadPendingTask will be created by unprotectedExecuteJob method and submit spark etl job.
 * Step2: LoadEtlChecker will check spark etl job status periodly and send push tasks to be when spark etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodly and commit transaction when push tasks are finished.
 * Step4: PublishVersionDaemon will send publish version tasks to be and finish transaction.
 */
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // for global dict
    public static final String BITMAP_DATA_PROPERTY = "bitmap_data";

    // --- members below need persist ---
    // create from resourceDesc when job created
    private SparkResource sparkResource;
    // members below updated when job state changed to etl
    private long etlStartTimestamp = -1;
    // for spark yarn
    private String appId = "";
    // spark job outputPath
    private String etlOutputPath = "";
    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    // --- members below not persist ---
    // temporary use
    // one SparkLoadJob has only one table to load
    // hivedb.table for global dict
    private String hiveTableName = "";
    private ResourceDesc resourceDesc;
    // for spark standalone
    private SparkAppHandle sparkAppHandle;
    // for straggler wait long time to commit transaction
    private long quorumFinishTimestamp = -1;
    // below for push task
    private Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();
    //private Map<Long, PushBrokerScannerParams> indexToPushBrokerReaderParams = Maps.newHashMap();
    private Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();
    private Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask = Maps.newHashMap();
    private Set<Long> finishedReplicas = Sets.newHashSet();
    private Set<Long> quorumTablets = Sets.newHashSet();
    private Set<Long> fullTablets = Sets.newHashSet();

    // only for log replay
    public SparkLoadJob() {
        super();
        jobType = EtlJobType.SPARK;
    }

    public SparkLoadJob(long dbId, String label, ResourceDesc resourceDesc, OriginStatement originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.resourceDesc = resourceDesc;
        timeoutSecond = Config.spark_load_default_timeout_second;
        jobType = EtlJobType.SPARK;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        // set spark resource and broker desc
        setResourceInfo();

        // global dict
        if (properties != null) {
            if (properties.containsKey(BITMAP_DATA_PROPERTY)) {
                hiveTableName = properties.get(BITMAP_DATA_PROPERTY);
            }
        }
    }

    /**
     * merge system conf with load stmt
     * @throws DdlException
     */
    private void setResourceInfo() throws DdlException {
        // spark resource
        String resourceName = resourceDesc.getName();
        Resource oriResource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        if (oriResource == null) {
            throw new DdlException("Resource does not exist. name: " + resourceName);
        }
        sparkResource = ((SparkResource) oriResource).getCopiedResource();
        sparkResource.update(resourceDesc);

        // broker desc
        Map<String, String> brokerProperties = sparkResource.getBrokerPropertiesWithoutPrefix();
        brokerDesc = new BrokerDesc(sparkResource.getBroker(), brokerProperties);
    }

    /**
     * load job already cancelled or finished, clear job below:
     * 1. kill etl job and delete etl files
     * 2. clear push tasks and infos that not persist
     */
    private void clearJob() {
        Preconditions.checkState(state == JobState.FINISHED || state == JobState.CANCELLED);

        LOG.debug("kill etl job and delete etl files. id: {}, state: {}", id, state);
        // TODO(wyb): spark-load
        //SparkEtlJobHandler handler = new SparkEtlJobHandler();
        if (state == JobState.CANCELLED) {
            if ((!Strings.isNullOrEmpty(appId) && sparkResource.isYarnMaster()) || sparkAppHandle != null) {
                try {
                    // TODO(wyb): spark-load
                    //handler.killEtlJob(sparkAppHandle, appId, id, sparkResource);
                } catch (Exception e) {
                    LOG.warn("kill etl job failed. id: {}, state: {}", id, state, e);
                }
            }
        }
        if (!Strings.isNullOrEmpty(etlOutputPath)) {
            try {
                // delete label dir, remove the last taskId dir
                String outputPath = etlOutputPath.substring(0, etlOutputPath.lastIndexOf("/"));
                // TODO(wyb): spark-load
                //handler.deleteEtlOutputPath(outputPath, brokerDesc);
            } catch (Exception e) {
                LOG.warn("delete etl files failed. id: {}, state: {}", id, state, e);
            }
        }

        LOG.debug("clear push tasks and infos that not persist. id: {}, state: {}", id, state);
        writeLock();
        try {
            // clear push task first
            for (Map<Long, PushTask> sentReplicaPushTask : tabletToSentReplicaPushTask.values()) {
                for (PushTask pushTask : sentReplicaPushTask.values()) {
                    if (pushTask == null) {
                        continue;
                    }
                    AgentTaskQueue.removeTask(pushTask.getBackendId(), pushTask.getTaskType(), pushTask.getSignature());
                }
            }
            // clear job infos that not persist
            hiveTableName = "";
            sparkAppHandle = null;
            resourceDesc = null;
            tableToLoadPartitions.clear();
            //indexToPushBrokerReaderParams.clear();
            indexToSchemaHash.clear();
            tabletToSentReplicaPushTask.clear();
            finishedReplicas.clear();
            quorumTablets.clear();
            fullTablets.clear();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        clearJob();
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        clearJob();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        sparkResource.write(out);
        out.writeLong(etlStartTimestamp);
        Text.writeString(out, appId);
        Text.writeString(out, etlOutputPath);
        out.writeInt(tabletMetaToFileInfo.size());
        for (Map.Entry<String, Pair<String, Long>> entry : tabletMetaToFileInfo.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().first);
            out.writeLong(entry.getValue().second);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        sparkResource = (SparkResource) Resource.read(in);
        etlStartTimestamp = in.readLong();
        appId = Text.readString(in);
        etlOutputPath = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tabletMetaStr = Text.readString(in);
            Pair<String, Long> fileInfo = Pair.create(Text.readString(in), in.readLong());
            tabletMetaToFileInfo.put(tabletMetaStr, fileInfo);
        }
    }
}
