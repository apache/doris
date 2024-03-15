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

package org.apache.doris.cloud.load;

import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.JobState;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudCopyJob extends BrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CloudCopyJob.class);
    private static final String TABLE_NAME_KEY = "TableName";
    private static final String USER_NAME_KEY = "UserName";

    @Getter
    private String stageId;
    @Getter
    private StagePB.StageType stageType;
    @Getter
    private String stagePrefix;
    @Getter
    private long sizeLimit;
    @Getter
    private String pattern;
    @Getter
    private ObjectInfo objectInfo;
    @Getter
    private String copyId;
    @Getter
    private boolean forceCopy;
    private String loadFilePaths = "";
    private Map<String, String> properties = new HashMap<>();
    private volatile boolean abortedCopy = false;
    private boolean isReplay = false;
    private List<String> loadFiles = null;

    public CloudCopyJob() {
        super(EtlJobType.COPY);
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        loadFiles = null;
        abortedCopy = true;
    }

    public void setAbortedCopy(boolean abortedCopy) {
        this.abortedCopy = abortedCopy;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, copyId);
        Text.writeString(out, loadFilePaths);
        Gson gson = new Gson();
        Text.writeString(out, properties == null ? "" : gson.toJson(properties));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        copyId = Text.readString(in);
        loadFilePaths = Text.readString(in);
        String property = Text.readString(in);
        properties = property.isEmpty() ? new HashMap<>()
                : (new Gson().fromJson(property, new TypeToken<Map<String, String>>() {
                }.getType()));

        // FIXME: COPY JOB is not supported yet.
        state = JobState.CANCELLED;
    }
}
