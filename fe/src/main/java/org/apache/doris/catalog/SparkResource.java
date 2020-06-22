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

import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
public class SparkResource extends Resource {
    private static final String SPARK_MASTER = "spark.master";
    private static final String SPARK_SUBMIT_DEPLOY_MODE = "spark.submit.deployMode";
    private static final String WORKING_DIR = "working_dir";
    private static final String BROKER = "broker";
    private static final String YARN_MASTER = "yarn";
    private static final String SPARK_CONFIG_PREFIX = "spark.";
    private static final String BROKER_PROPERTY_PREFIX = "broker.";
    // spark uses hadoop configs in the form of spark.hadoop.*
    private static final String SPARK_YARN_RESOURCE_MANAGER_ADDRESS = "spark.hadoop.yarn.resourcemanager.address";
    private static final String SPARK_FS_DEFAULT_FS = "spark.hadoop.fs.defaultFS";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.address";

    public enum DeployMode {
        CLUSTER,
        CLIENT;

        public static DeployMode fromString(String deployMode) {
            for (DeployMode mode : DeployMode.values()) {
                if (mode.name().equalsIgnoreCase(deployMode)) {
                    return mode;
                }
            }
            return null;
        }
    }

    @SerializedName(value = "sparkConfigs")
    private Map<String, String> sparkConfigs;
    @SerializedName(value = "workingDir")
    private String workingDir;
    @SerializedName(value = "broker")
    private String broker;
    // broker username and password
    @SerializedName(value = "brokerProperties")
    private Map<String, String> brokerProperties;

    public SparkResource(String name) {
        this(name, Maps.newHashMap(), null, null, Maps.newHashMap());
    }

    private SparkResource(String name, Map<String, String> sparkConfigs, String workingDir, String broker,
                          Map<String, String> brokerProperties) {
        super(name, ResourceType.SPARK);
        this.sparkConfigs = sparkConfigs;
        this.workingDir = workingDir;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
    }

    public String getMaster() {
        return sparkConfigs.get(SPARK_MASTER);
    }

    public DeployMode getDeployMode() {
        return DeployMode.fromString(sparkConfigs.get(SPARK_SUBMIT_DEPLOY_MODE));
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public String getBroker() {
        return broker;
    }

    public Map<String, String> getBrokerPropertiesWithoutPrefix() {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : brokerProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(BROKER_PROPERTY_PREFIX)) {
                properties.put(key.substring(key.indexOf(".") + 1), entry.getValue());
            }
        }
        return properties;
    }

    public Map<String, String> getSparkConfigs() {
        return sparkConfigs;
    }

    public Pair<String, String> getYarnResourcemanagerAddressPair() {
        return Pair.create(YARN_RESOURCE_MANAGER_ADDRESS, sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_ADDRESS));
    }

    public SparkResource getCopiedResource() {
        return new SparkResource(name, Maps.newHashMap(sparkConfigs), workingDir, broker, brokerProperties);
    }

    public boolean isYarnMaster() {
        return getMaster().equalsIgnoreCase(YARN_MASTER);
    }

    public void update(ResourceDesc resourceDesc) throws DdlException {
        Preconditions.checkState(name.equals(resourceDesc.getName()));

        Map<String, String> properties = resourceDesc.getProperties();
        if (properties == null) {
            return;
        }

        // update spark configs
        if (properties.containsKey(SPARK_MASTER)) {
            throw new DdlException("Cannot change spark master");
        }
        sparkConfigs.putAll(getSparkConfigs(properties));

        // update working dir and broker
        if (properties.containsKey(WORKING_DIR)) {
            workingDir = properties.get(WORKING_DIR);
        }
        if (properties.containsKey(BROKER)) {
            broker = properties.get(BROKER);
        }
        brokerProperties.putAll(getBrokerProperties(properties));
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        // get spark configs
        sparkConfigs = getSparkConfigs(properties);
        // check master and deploy mode
        if (getMaster() == null) {
            throw new DdlException("Missing " + SPARK_MASTER + " in properties");
        }
        String deployModeStr = sparkConfigs.get(SPARK_SUBMIT_DEPLOY_MODE);
        if (deployModeStr != null) {
            DeployMode deployMode = DeployMode.fromString(deployModeStr);
            if (deployMode == null) {
                throw new DdlException("Unknown deploy mode: " + deployModeStr);
            }
        } else {
            throw new DdlException("Missing " + SPARK_SUBMIT_DEPLOY_MODE + " in properties");
        }
        // if deploy machines do not set HADOOP_CONF_DIR env, we should set these configs blow
        if ((!sparkConfigs.containsKey(SPARK_YARN_RESOURCE_MANAGER_ADDRESS) || !sparkConfigs.containsKey(SPARK_FS_DEFAULT_FS))
                && isYarnMaster()) {
            throw new DdlException("Missing (" + SPARK_YARN_RESOURCE_MANAGER_ADDRESS + " and " + SPARK_FS_DEFAULT_FS
                                           + ") in yarn master");
        }

        // check working dir and broker
        workingDir = properties.get(WORKING_DIR);
        broker = properties.get(BROKER);
        if ((workingDir == null && broker != null) || (workingDir != null && broker == null)) {
            throw new DdlException("working_dir and broker should be assigned at the same time");
        }
        // check broker exist
        if (broker != null && !Catalog.getCurrentCatalog().getBrokerMgr().contaisnBroker(broker)) {
            throw new DdlException("Unknown broker name(" + broker + ")");
        }
        brokerProperties = getBrokerProperties(properties);
    }

    private Map<String, String> getSparkConfigs(Map<String, String> properties) {
        Map<String, String> sparkConfigs = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SPARK_CONFIG_PREFIX)) {
                sparkConfigs.put(entry.getKey(), entry.getValue());
            }
        }
        return sparkConfigs;
    }

    private Map<String, String> getBrokerProperties(Map<String, String> properties) {
        Map<String, String> brokerProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(BROKER_PROPERTY_PREFIX)) {
                brokerProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return brokerProperties;
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : sparkConfigs.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
        if (workingDir != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, SparkResource.WORKING_DIR, workingDir));
        }
        if (broker != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, SparkResource.BROKER, broker));
        }
        for (Map.Entry<String, String> entry : brokerProperties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
