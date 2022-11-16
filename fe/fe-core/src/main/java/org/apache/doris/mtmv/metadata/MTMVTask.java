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

package org.apache.doris.mtmv.metadata;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mtmv.MTMVUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MTMVTask implements Writable, Comparable {
    // also named query id in ConnectContext
    @SerializedName("taskId")
    private String taskId;

    @SerializedName("jobName")
    private String jobName;

    @SerializedName("createTime")
    private long createTime;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("state")
    private MTMVUtils.TaskState state = MTMVUtils.TaskState.PENDING;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("mvName")
    private String mvName;

    @SerializedName("query")
    private String query;

    @SerializedName("user")
    private String user;

    @SerializedName("message")
    private String message;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("expireTime")
    private long expireTime;

    // the larger the value, the higher the priority, the default value is 0
    @SerializedName("priority")
    private int priority = 0;

    @SerializedName("retryTimes")
    private int retryTimes = 0;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public MTMVUtils.TaskState getState() {
        return state;
    }

    public void setState(MTMVUtils.TaskState state) {
        this.state = state;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getMvName() {
        return mvName;
    }

    public void setMvName(String mvName) {
        this.mvName = mvName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQuery() {
        return query == null ? "" : query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message == null ? "" : message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public static MTMVTask read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MTMVTask.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static final ImmutableList<String> SHOW_TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("TaskId")
                    .add("JobName")
                    .add("DBName")
                    .add("MVName")
                    .add("Query")
                    .add("User")
                    .add("Priority")
                    .add("RetryTimes")
                    .add("State")
                    .add("Message")
                    .add("ErrorCode")
                    .add("CreateTime")
                    .add("ExpireTime")
                    .add("FinishTime")
                    .build();

    public List<String> toStringRow() {
        List<String> list = Lists.newArrayList();
        list.add(getTaskId());
        list.add(getJobName());
        list.add(getDbName());
        list.add(getMvName());
        list.add(getQuery().length() > 10240 ? getQuery().substring(0, 10240) : getQuery());
        list.add(getUser());
        list.add(Integer.toString(getPriority()));
        list.add(Integer.toString(getRetryTimes()));
        list.add(getState().toString());
        list.add(getMessage().length() > 10240 ? getMessage().substring(0, 10240) : getMessage());
        list.add(Integer.toString(getErrorCode()));
        list.add(MTMVUtils.getTimeString(getCreateTime()));
        list.add(MTMVUtils.getTimeString(getExpireTime()));
        list.add(MTMVUtils.getTimeString(getFinishTime()));
        return list;
    }

    @Override
    public int compareTo(@NotNull Object o) {
        return (int) (getCreateTime() - ((MTMVTask) o).getCreateTime());
    }
}
