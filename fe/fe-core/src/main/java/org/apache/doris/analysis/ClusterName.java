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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterName implements Writable {

    private String cluster;
    private String db;

    public ClusterName(String cluster, String db) {
        this.cluster = cluster;
        this.db = db;
    }

    public String getCluster() {
        return cluster;
    }

    public String getDb() {
        return db;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(cluster)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_CLUSTER_NAME);
        }

        if (Strings.isNullOrEmpty(db)) {
            throw new AnalysisException("db name is null");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, cluster);
        Text.writeString(out, db);
    }

    public void readFields(DataInput in) throws IOException {
        cluster = Text.readString(in);
        db = Text.readString(in);
    }
}
