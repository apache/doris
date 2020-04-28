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

import com.google.common.base.Strings;
import org.apache.commons.lang.NotImplementedException;
import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.EtlCluster.EtlClusterType;
import org.apache.doris.common.AnalysisException;

import java.util.Map;

// ALTER SYSTEM ADD LOAD CLUSTER "cluster0"
// PROPERTIES
// (
//     "type" = "spark",
//     "master" = "yarn",
//     "deploy_mode" = "cluster",
//     "spark_args" = "--jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb",
//     "spark_configs" = "spark.driver.memory=1g;spark.executor.memory=1g",
//     "yarn_configs" = "yarn.resourcemanager.address=host:port;fs.defaultFS=hdfs://host:port",
//     "hdfs_etl_path" = "hdfs://127.0.0.1:10000/tmp/doris",
//     "broker" = "broker0"
// );
//
// ALTER SYSTEM DROP LOAD CLUSTER "cluster0";
public class ModifyEtlClusterClause extends AlterClause {
    private static final String TYPE = "type";

    public enum ModifyOp {
        OP_ADD,
        OP_DROP
    }

    private final ModifyOp op;
    private final String clusterName;
    private final Map<String, String> properties;
    private EtlClusterType clusterType;

    // add cluster
    public ModifyEtlClusterClause(ModifyOp op, String clusterName, Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.op = op;
        this.clusterName = clusterName;
        this.properties = properties;
    }

    // drop cluster
    public ModifyEtlClusterClause(ModifyOp op, String clusterName) {
        this(op, clusterName, null);
    }

    public static ModifyEtlClusterClause createAddEtlClusterClause(String clusterName, Map<String, String> properties) {
        return new ModifyEtlClusterClause(ModifyOp.OP_ADD, clusterName, properties);
    }

    public static ModifyEtlClusterClause createDropEtlClusterClause(String clusterName) {
        return new ModifyEtlClusterClause(ModifyOp.OP_DROP, clusterName);
    }

    public ModifyOp getOp() {
        return op;
    }

    public String getClusterName() {
        return clusterName;
    }

    public EtlClusterType getClusterType() {
        return clusterType;
    }

    private void validateEtlClusterName() throws AnalysisException {
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new AnalysisException("Etl cluster name can't be empty.");
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        validateEtlClusterName();

        if (op == ModifyOp.OP_DROP) {
            return;
        }

        // check properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Etl cluster properties can't be null.");
        }

        String type = properties.get(TYPE);
        if (type == null) {
            throw new AnalysisException("Etl cluster type can't be null.");
        }
        if (!type.equalsIgnoreCase(EtlClusterType.SPARK.name())) {
            throw new AnalysisException("Only support Spark cluster.");
        }
        clusterType = EtlClusterType.fromString(type);
    }

    @Override
    public String toSql() {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
