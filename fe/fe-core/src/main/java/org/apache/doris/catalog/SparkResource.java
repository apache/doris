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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Spark resource for etl or query.
 * working_dir and broker[.xxx] are optional and used in spark ETL.
 * working_dir is used to store ETL intermediate files and broker is used to read the intermediate files by BE.
 *
 * Spark resource example:
 * CREATE EXTERNAL RESOURCE "spark0"
 * PROPERTIES
 * (
 *     "type" = "spark",
 *     "spark.master" = "yarn",
 *     "spark.submit.deployMode" = "cluster",
 *     "spark.jars" = "xxx.jar,yyy.jar",
 *     "spark.files" = "/tmp/aaa,/tmp/bbb",
 *     "spark.executor.memory" = "1g",
 *     "spark.yarn.queue" = "queue0",
 *     "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
 *     "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
 *     "working_dir" = "hdfs://127.0.0.1:10000/tmp/doris",
 *     "broker" = "broker0",
 *     "broker.username" = "user0",
 *     "broker.password" = "password0"
 * );
 *
 * DROP RESOURCE "spark0";
 */
@Deprecated
public class SparkResource extends Resource {

    @SerializedName(value = "sparkConfigs")
    private Map<String, String> sparkConfigs;
    @SerializedName(value = "workingDir")
    private String workingDir;
    @SerializedName(value = "broker")
    private String broker;
    @SerializedName(value = "brokerProperties")
    private Map<String, String> brokerProperties;
    @SerializedName(value = "envConfigs")
    private Map<String, String> envConfigs;

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap();
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> properties) throws DdlException {
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
    }
}
