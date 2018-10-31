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

package org.apache.doris.backup;

import org.apache.doris.analysis.LabelName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Deprecated
public class AbstractBackupJob_D implements Writable {
    private static final Logger LOG = LogManager.getLogger(AbstractBackupJob_D.class);

    protected long jobId;
    protected long dbId;
    protected LabelName labelName;

    protected String errMsg;
    protected String remotePath;
    protected Map<String, String> remoteProperties;

    // unfinished tabletId (reuse for snapshot, upload)
    // tabletId -> backendId
    protected Multimap<Long, Long> unfinishedTabletIds;

    protected long createTime;
    protected long finishedTime;

    protected PathBuilder pathBuilder;
    protected CommandBuilder commandBuilder;

    protected Future<String> future;

    public AbstractBackupJob_D() {
        unfinishedTabletIds = HashMultimap.create();
    }

    public AbstractBackupJob_D(long jobId, long dbId, LabelName labelName, String remotePath,
                             Map<String, String> remoteProperties) {
        this.jobId = jobId;
        this.dbId = dbId;
        this.labelName = labelName;
        this.remotePath = remotePath;
        this.remoteProperties = remoteProperties;
        this.unfinishedTabletIds = HashMultimap.create();

        this.errMsg = "";
        this.createTime = System.currentTimeMillis();
        this.finishedTime = -1L;
    }

    public long getJobId() {
        return jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public String getLocalDirName() {
        return getDbName() + "_" + getLabel();
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public String getRemotePath() {
        return remotePath;
    }

    public Map<String, String> getRemoteProperties() {
        return remoteProperties;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public List<Comparable> getJobInfo() {
        throw new NotImplementedException();
    }

    public synchronized int getLeftTasksNum() {
        if (unfinishedTabletIds != null) {
            return unfinishedTabletIds.size();
        } else {
            return -1;
        }
    }

    public void end(Catalog catalog, boolean isReplay) {
        throw new NotImplementedException();
    }

    public void runOnce() {
        throw new NotImplementedException();
    }

    public synchronized List<List<Comparable>> getUnfinishedInfos() {
        List<List<Comparable>> infos = Lists.newArrayList();
        if (unfinishedTabletIds != null) {
            for (Long tabletId : unfinishedTabletIds.keySet()) {
                Collection<Long> backendIds = unfinishedTabletIds.get(tabletId);
                List<Comparable> info = Lists.newArrayList();
                info.add(tabletId);
                info.add(Joiner.on(",").join(backendIds));
                infos.add(info);
            }
        }
        return infos;
    }

    protected void clearJob() {
        throw new NotImplementedException();
    }

    protected boolean checkFuture(String msg) throws InterruptedException, ExecutionException, IOException {
        if (!future.isDone()) {
            LOG.info("waiting {} finished. job: {}", msg, jobId);
            return false;
        } else {
            String errMsg = future.get();
            // reset future;
            future = null;
            if (errMsg != null) {
                throw new IOException("failed to " + msg + " [" + errMsg + "]. backup job[" + jobId + "]");
            } else {
                LOG.info("{} finished. job: {}", msg, jobId);
                return true;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(dbId);
        labelName.write(out);

        Text.writeString(out, errMsg);
        Text.writeString(out, remotePath);

        if (remoteProperties == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int size = remoteProperties.size();
            out.writeInt(size);
            for (Map.Entry<String, String> entry : remoteProperties.entrySet()) {
                Text.writeString(out, entry.getKey());
                Text.writeString(out, entry.getValue());
            }
        }

        out.writeLong(createTime);
        out.writeLong(finishedTime);

        if (pathBuilder == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            pathBuilder.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        jobId = in.readLong();
        dbId = in.readLong();
        labelName = new LabelName();
        labelName.readFields(in);

        errMsg = Text.readString(in);
        remotePath = Text.readString(in);

        if (in.readBoolean()) {
            remoteProperties = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                remoteProperties.put(key, value);
            }
        }

        createTime = in.readLong();
        finishedTime = in.readLong();

        if (in.readBoolean()) {
            pathBuilder = new PathBuilder();
            pathBuilder.readFields(in);
        }
    }
}
