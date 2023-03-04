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

package org.apache.doris.sparkdpp;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class DppResult implements Serializable {
    public DppResult() {
        isSuccess = true;
        failedReason = "";
        scannedRows = 0;
        fileNumber = 0;
        fileSize = 0;
        normalRows = 0;
        abnormalRows = 0;
        unselectRows = 0;
        partialAbnormalRows = "";
        scannedBytes = 0;
    }

    @SerializedName("is_success")
    public boolean isSuccess;

    @SerializedName("failed_reason")
    public String failedReason;

    @SerializedName("scanned_rows")
    public long scannedRows;

    @SerializedName("file_number")
    public long fileNumber;

    @SerializedName("file_size")
    public long fileSize;

    @SerializedName("normal_rows")
    public long normalRows;

    @SerializedName("abnormal_rows")
    public long abnormalRows;

    @SerializedName("unselect_rows")
    public long unselectRows;

    // only part of abnormal rows will be returned
    @SerializedName("partial_abnormal_rows")
    public String partialAbnormalRows;

    @SerializedName("scanned_bytes")
    public long scannedBytes;
}
