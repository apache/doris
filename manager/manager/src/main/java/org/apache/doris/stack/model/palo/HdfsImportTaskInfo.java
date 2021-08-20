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

package org.apache.doris.stack.model.palo;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.response.construct.NativeQueryResp;
import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
public class HdfsImportTaskInfo {

    private String jobId;

    private String label;

    private String state;

    private String progress;

    private String type;

    private String etlInfo;

    private String taskInfo;

    private String errorMsg;

    private Timestamp createTime;

    private Timestamp etlStartTime;

    private Timestamp etlFinishTime;

    private Timestamp loadStartTime;

    private Timestamp loadFinishTime;

    private String url;

    private JobDetails jobDetails;

    /**
     * JobDetails
     */
    @Data
    public static class JobDetails {

        private int ScannedRows;

        private int TaskNumber;

        private int FileNumber;

        private long FileSize;

        private String UnfinishedBackends;

        private String AllBackends;
    }

    public HdfsImportTaskInfo(NativeQueryResp result) {
        List<NativeQueryResp.Meta> metaList = result.getMeta();
        List<String> data = result.getData().get(0);
        int index = 0;
        for (NativeQueryResp.Meta meta : metaList) {
            if (data.get(index) == null || data.get(index).isEmpty()) {
                index++;
                continue;
            }
            switch (meta.getName()) {
                case "JobId":
                    this.jobId = data.get(index);
                    break;
                case "Label":
                    this.label = data.get(index);
                    break;
                case "State":
                    this.state = data.get(index);
                    break;
                case "Progress":
                    this.progress = data.get(index);
                    break;
                case "Type":
                    this.type = data.get(index);
                    break;
                case "EtlInfo":
                    this.etlInfo = data.get(index);
                    break;
                case "TaskInfo":
                    this.taskInfo = data.get(index);
                    break;
                case "ErrorMsg":
                    this.errorMsg = data.get(index);
                    break;
                case "CreateTime":
                    this.createTime = Timestamp.valueOf(data.get(index));
                    break;
                case "EtlStartTime":
                    this.etlStartTime = Timestamp.valueOf(data.get(index));
                    break;
                case "EtlFinishTime":
                    this.etlFinishTime = Timestamp.valueOf(data.get(index));
                    break;
                case "LoadStartTime":
                    this.loadStartTime = Timestamp.valueOf(data.get(index));
                    break;
                case "LoadFinishTime":
                    this.loadFinishTime = Timestamp.valueOf(data.get(index));
                    break;
                case "URL":
                    this.url = data.get(index);
                    break;
                case "JobDetails":
                    String details = data.get(index).replaceAll(" ", "");
                    this.jobDetails = JSON.parseObject(details, JobDetails.class);
                    break;
                default:
                    break;
            }
            index++;
        }
    }
}
