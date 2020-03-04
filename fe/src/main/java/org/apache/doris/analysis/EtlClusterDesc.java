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
import org.apache.doris.load.EtlJobType;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

// Etl cluster descriptor
public class EtlClusterDesc extends DataProcessorDesc {
    private static final String SPARK_CLUSTER_NAME_PREFIX = "spark.";

    private EtlClusterDesc() {
        this(null, null);
    }

    public EtlClusterDesc(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void analyze() throws AnalysisException {
        super.analyze();

        // analyze name
        String clusterName = getName().toLowerCase();
        if (!clusterName.startsWith(SPARK_CLUSTER_NAME_PREFIX) || clusterName.split("\\.").length != 2) {
            throw new AnalysisException("Etl cluster name should be 'spark.{cluster_name}'");
        }

        // check etl cluster exist or not
    }

    @Override
    public EtlJobType getEtlJobType() {
        String clusterName = getName().toLowerCase();
        if (clusterName.startsWith(SPARK_CLUSTER_NAME_PREFIX)) {
            return EtlJobType.SPARK;
        }

        return null;
    }

    public static EtlClusterDesc read(DataInput in) throws IOException {
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc();
        etlClusterDesc.readFields(in);
        return etlClusterDesc;
    }
}