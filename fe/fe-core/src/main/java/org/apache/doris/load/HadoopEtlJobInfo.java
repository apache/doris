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

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HadoopEtlJobInfo extends EtlJobInfo {
    private String cluster;
    private String etlJobId;
    // etl output path: /outputPath/dbId/label/etlOutputDir
    private String etlOutputDir;

    private DppConfig dppConfig;

    public HadoopEtlJobInfo() {
        super();
        this.cluster = Config.dpp_default_cluster;
        this.etlJobId = "";
        this.etlOutputDir = String.valueOf(System.currentTimeMillis());
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getEtlJobId() {
        return etlJobId;
    }

    public void setEtlJobId(String etlJobId) {
        this.etlJobId = etlJobId;
    }

    public String getEtlOutputDir() {
        return etlOutputDir;
    }

    public void setEtlOutputDir(String etlOutputDir) {
        this.etlOutputDir = etlOutputDir;
    }

    public DppConfig getDppConfig() {
        return dppConfig;
    }

    public void setDppConfig(DppConfig dppConfig) {
        this.dppConfig = dppConfig;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, cluster);
        Text.writeString(out, etlJobId);
        Text.writeString(out, etlOutputDir);

        if (dppConfig == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            dppConfig.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        cluster = Text.readString(in);
        etlJobId = Text.readString(in);
        etlOutputDir = Text.readString(in);
        if (in.readBoolean()) {
            dppConfig = new DppConfig();
            dppConfig.readFields(in);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof HadoopEtlJobInfo)) {
            return false;
        }
        
        HadoopEtlJobInfo jobInfo = (HadoopEtlJobInfo) obj;

        if (dppConfig != jobInfo.dppConfig) {
            if (dppConfig == null || jobInfo.dppConfig == null) {
                return false;
            }

            if (!dppConfig.equals(jobInfo.dppConfig)) {
                return false;
            }
        }
 
        return cluster.equals(jobInfo.cluster)
                && etlJobId.equals(jobInfo.etlJobId)
                && etlOutputDir.equals(jobInfo.etlOutputDir);
    }
}
