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

package org.apache.doris.stack.model.response.construct;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataImportTaskPageResp {
    private int page;

    private int pageSize;

    private List<DataImportTaskResp> taskRespList;

    private int totalSize;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataImportTaskResp {
        private long taskId;

        private String taskName;

        private String creator;

        private Timestamp createTime;

        private String importType;

        private String destTableName;

        private String status;

        private String connectInfo;

        private String fileInfo;

        private int fileNumber;

        private long totalRows;

        private long loadRows;

        private long filteredRows;

        private long unselectedRows;

        private long loadTimeMs;

        private long loadBytes;

        private String errorMsg;

        private String errorInfo;
    }

    /**
     * 导入状态
     */
    public enum Status {
        PENDING,
        LOADING,
        CANCELLED,
        FINISHED
    }
}
