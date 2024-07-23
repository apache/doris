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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

// Resource descriptor
//
// Spark example:
// WITH RESOURCE "spark0"
// (
//   "spark.jars" = "xxx.jar,yyy.jar",
//   "spark.files" = "/tmp/aaa,/tmp/bbb",
//   "spark.executor.memory" = "1g",
//   "spark.yarn.queue" = "queue0"
// )
public class ResourceDesc {
    @SerializedName("nm")
    protected String name;
    @SerializedName("prop")
    protected Map<String, String> properties;
    /**
     * TODO(tsy): transfer to LoadType
     */
    protected EtlJobType etlJobType;

    protected LoadType loadType;

    // Only used for recovery
    public ResourceDesc() {
    }

    public ResourceDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.etlJobType = EtlJobType.UNKNOWN;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public void analyze() throws AnalysisException {
        // check resource exist or not
        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(getName());
        if (resource == null) {
            throw new AnalysisException("Resource does not exist. name: " + getName());
        }
        if (resource.getType() == Resource.ResourceType.SPARK) {
            etlJobType = EtlJobType.SPARK;
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("WITH RESOURCE '").append(name).append("'");
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap.toString()).append(")");
        }
        return sb.toString();
    }
}
