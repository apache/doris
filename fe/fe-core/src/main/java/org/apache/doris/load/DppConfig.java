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

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TPriority;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

@Deprecated
public class DppConfig {
    // palo base path in hadoop
    //   dpp: paloPath/cluster_id/applications/dpp_version
    //   output: paloPath/cluster_id/output
    @SerializedName(value = "paloPath")
    private String paloPath;
    @SerializedName(value = "httpPort")
    private int httpPort;
    @SerializedName(value = "hadoopConfigs")
    private Map<String, String> hadoopConfigs;
    // priority for palo internal schedule
    // for now are etl submit schedule and download file schedule
    @SerializedName(value = "priority")
    private TPriority priority;

    // for persist
    public DppConfig() {
        this(null, -1, null, null);
    }

    private DppConfig(String paloPath, int httpPort, Map<String, String> hadoopConfigs, TPriority priority) {
        this.paloPath = paloPath;
        this.httpPort = httpPort;
        this.hadoopConfigs = hadoopConfigs;
        this.priority = priority;
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        boolean readPaloPath = false;
        if (in.readBoolean()) {
            readPaloPath = true;
        }
        if (readPaloPath) {
            paloPath = Text.readString(in);
        }

        httpPort = in.readInt();

        boolean readHadoopConfigs = false;
        if (in.readBoolean()) {
            readHadoopConfigs = true;
        }
        if (readHadoopConfigs) {
            hadoopConfigs = Maps.newHashMap();
            int count = in.readInt();
            for (int i = 0; i < count; ++i) {
                hadoopConfigs.put(Text.readString(in), Text.readString(in));
            }
        }

        this.priority = TPriority.valueOf(Text.readString(in));
    }
}
