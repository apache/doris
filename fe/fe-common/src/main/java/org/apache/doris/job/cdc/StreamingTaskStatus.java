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

package org.apache.doris.job.cdc;

// cdc_client -> FE running task status, pulled by the FE only when the local timeout
// budget is already exceeded. scannedRows is the read-end heartbeat used to renew the
// deadline (scannedRows < 0 means no progress info: not scanning, or scan finished);
// failReason carries the recorded write error so a kill can report the real cause.
public class StreamingTaskStatus {
    private long scannedRows = -1;
    private String failReason = "";

    public StreamingTaskStatus() {}

    public StreamingTaskStatus(long scannedRows, String failReason) {
        this.scannedRows = scannedRows;
        this.failReason = failReason;
    }

    public long getScannedRows() {
        return scannedRows;
    }

    public void setScannedRows(long scannedRows) {
        this.scannedRows = scannedRows;
    }

    public String getFailReason() {
        return failReason;
    }

    public void setFailReason(String failReason) {
        this.failReason = failReason;
    }
}
