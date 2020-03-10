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

import com.google.common.collect.Maps;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.EtlJobType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// Etl cluster with broker descriptor
public class EtlClusterWithBrokerDesc implements Writable {
    private static final String SPARK_CLUSTER_NAME_PREFIX = "spark.";

    private String clusterName;
    private String brokerName;
    private Map<String, String> properties;

    // Only used for recovery
    private EtlClusterWithBrokerDesc() {
    }

    public EtlClusterWithBrokerDesc(String clusterName, String brokerName, Map<String, String> properties) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc(brokerName, properties);
    }

    public void analyze() throws AnalysisException {
        // analyze name
        String lowerCaseClusterName = clusterName.toLowerCase();
        if (!lowerCaseClusterName.startsWith(SPARK_CLUSTER_NAME_PREFIX)
                || lowerCaseClusterName.split("\\.").length != 2) {
            throw new AnalysisException("Etl cluster name should be 'spark.{cluster_name}'");
        }

        // check etl cluster exist or not
    }

    public EtlJobType getEtlJobType() {
        String lowerCaseClusterName = clusterName.toLowerCase();
        if (lowerCaseClusterName.startsWith(SPARK_CLUSTER_NAME_PREFIX)) {
            return EtlJobType.SPARK;
        }

        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, clusterName);
        Text.writeString(out, brokerName);
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        clusterName = Text.readString(in);
        brokerName = Text.readString(in);
        int size = in.readInt();
        properties = Maps.newHashMap();
        for (int i = 0; i < size; ++i) {
            final String key = Text.readString(in);
            final String val = Text.readString(in);
            properties.put(key, val);
        }
    }

    public static EtlClusterWithBrokerDesc read(DataInput in) throws IOException {
        EtlClusterWithBrokerDesc desc = new EtlClusterWithBrokerDesc();
        desc.readFields(in);
        return desc;
    }
}