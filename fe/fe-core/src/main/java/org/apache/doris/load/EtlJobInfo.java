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

package org.apache.doris.load;

import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EtlJobInfo implements Writable {
    private EtlStatus jobStatus;

    public EtlJobInfo() {
        jobStatus = new EtlStatus();
    }

    public EtlStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(EtlStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public String toString() {
        return "EtlJobInfo [jobStatus=" + jobStatus + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        jobStatus.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        jobStatus.readFields(in);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof EtlJobInfo) {
            EtlJobInfo other = (EtlJobInfo) obj;
            return jobStatus.equals(other.jobStatus);
        }
        return false;
    }
}
