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

//import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

public class SparkEtlCluster extends EtlCluster {
    private static final String SEMICOLON_SEPARATOR = ";";
    private static final String EQUAL_SEPARATOR = "=";
    private static final String COMMA_SEPARATOR = ",";

    private static final String MASTER = "master";
    private static final String DEPLOY_MODE = "deploy_mode";
    private static final String HDFS_ETL_PATH = "hdfs_etl_path";
    private static final String BROKER = "broker";
    private static final String SPARK_ARGS = "spark_args";
    private static final String SPARK_CONFIGS = "spark_configs";
    private static final String YARN_CONFIGS = "yarn_configs";

    private static final String SPARK_ARG_PREFIX = "--";
    private static final String SPARK_CONFIG_PREFIX = "spark.";
    private static final String YARN_MASTER = "yarn";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.address";
    private static final String FS_DEFAULT_FS = "fs.defaultFS";

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

    @SerializedName(value = MASTER)
    private String master;
    @SerializedName(value = DEPLOY_MODE)
    private DeployMode deployMode = DeployMode.CLUSTER;
    @SerializedName(value = HDFS_ETL_PATH)
    private String hdfsEtlPath;
    @SerializedName(value = BROKER)
    private String broker;
    @SerializedName(value = SPARK_ARGS)
    private Map<String, String> sparkArgsMap;
    @SerializedName(value = SPARK_CONFIGS)
    private Map<String, String> sparkConfigsMap;
    @SerializedName(value = YARN_CONFIGS)
    private Map<String, String> yarnConfigsMap;

    public SparkEtlCluster(String name) {
        this(name, null, DeployMode.CLUSTER, null, null, Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap());
    }

    private SparkEtlCluster(String name, String master, DeployMode deployMode, String hdfsEtlPath, String broker,
                            Map<String, String> sparkArgsMap, Map<String, String> sparkConfigsMap,
                            Map<String, String> yarnConfigsMap) {
        super(name, EtlClusterType.SPARK);
        this.master = master;
        this.deployMode = deployMode;
        this.hdfsEtlPath = hdfsEtlPath;
        this.broker = broker;
        this.sparkArgsMap = sparkArgsMap;
        this.sparkConfigsMap = sparkConfigsMap;
        this.yarnConfigsMap = yarnConfigsMap;
    }

    public String getMaster() {
        return master;
    }

    public DeployMode getDeployMode() {
        return deployMode;
    }

    public String getHdfsEtlPath() {
        return hdfsEtlPath;
    }

    public String getBroker() {
        return broker;
    }

    public Map<String, String> getSparkArgsMap() {
        return sparkArgsMap;
    }

    public Map<String, String> getSparkConfigsMap() {
        return sparkConfigsMap;
    }

    public Map<String, String> getYarnConfigsMap() {
        return yarnConfigsMap;
    }

    public SparkEtlCluster getCopiedEtlCluster() {
        return new SparkEtlCluster(name, master, deployMode, hdfsEtlPath, broker, Maps.newHashMap(sparkArgsMap),
                                   Maps.newHashMap(sparkConfigsMap), Maps.newHashMap(yarnConfigsMap));
    }

    public boolean isYarnMaster() {
        return master.equalsIgnoreCase(YARN_MASTER);
    }

    /*
    public void update(EtlClusterDesc etlClusterDesc) throws DdlException {
        Preconditions.checkState(name.equals(etlClusterDesc.getName()));

        Map<String, String> properties = etlClusterDesc.getProperties();
        if (properties == null) {
            return;
        }

        // replace master, deployMode, hdfsEtlPath, broker if exist in etlClusterDesc
        if (properties.containsKey(MASTER)) {
            master = properties.get(MASTER);
        }
        if (properties.containsKey(DEPLOY_MODE)) {
            DeployMode mode = DeployMode.fromString(properties.get(DEPLOY_MODE));
            if (mode == null) {
                throw new DdlException("Unknown deploy mode: " + properties.get(DEPLOY_MODE));
            }
            deployMode = mode;
        }
        if (properties.containsKey(HDFS_ETL_PATH)) {
            hdfsEtlPath = properties.get(hdfsEtlPath);
        }
        if (properties.containsKey(broker)) {
            broker = properties.get(broker);
        }

        // merge spark args
        if (properties.containsKey(SPARK_ARGS)) {
            Map<String, String> newArgsMap = getConfigsMap(properties, SPARK_ARGS);
            for (Map.Entry<String, String> entry : newArgsMap.entrySet()) {
                String argKey = entry.getKey();
                if (sparkArgsMap.containsKey(argKey)) {
                    sparkArgsMap.put(argKey, sparkArgsMap.get(argKey) + COMMA_SEPARATOR + entry.getValue());
                } else {
                    sparkArgsMap.put(argKey, entry.getValue());
                }
            }
        }

        // merge spark configs
        if (properties.containsKey(SPARK_CONFIGS)) {
            sparkConfigsMap.putAll(getSparkConfigsMap(properties, SPARK_CONFIGS));
        }

        // merge yarn configs
        if (properties.containsKey(YARN_CONFIGS)) {
            yarnConfigsMap.putAll(getYarnConfigsMap(properties, YARN_CONFIGS));
        }
    }
    */

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        master = properties.get(MASTER);
        if (master == null) {
            throw new DdlException("Missing spark master in properties");
        }
        DeployMode mode = DeployMode.fromString(properties.get(DEPLOY_MODE));
        if (mode != null) {
            deployMode = mode;
        }
        hdfsEtlPath = properties.get(HDFS_ETL_PATH);
        if (hdfsEtlPath == null) {
            throw new DdlException("Missing hdfs etl path in properties");
        }

        broker = properties.get(BROKER);
        if (broker == null) {
            throw new DdlException("Missing broker in properties");
        }
        // check broker exist
        if (!Catalog.getInstance().getBrokerMgr().contaisnBroker(broker)) {
            throw new DdlException("Unknown broker name(" + broker + ")");
        }

        sparkArgsMap = getSparkArgsMap(properties, SPARK_ARGS);
        sparkConfigsMap = getSparkConfigsMap(properties, SPARK_CONFIGS);
        yarnConfigsMap = getYarnConfigsMap(properties, YARN_CONFIGS);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, SparkEtlCluster.MASTER, master));
        result.addRow(Lists.newArrayList(name, lowerCaseType, SparkEtlCluster.DEPLOY_MODE,
                                         deployMode.name().toLowerCase()));
        result.addRow(Lists.newArrayList(name, lowerCaseType, SparkEtlCluster.HDFS_ETL_PATH, hdfsEtlPath));
        result.addRow(Lists.newArrayList(name, lowerCaseType, SparkEtlCluster.BROKER, broker));

        Map<String, Map<String, String>> keyToConfigsMap = Maps.newHashMap();
        keyToConfigsMap.put(SPARK_ARGS, sparkArgsMap);
        keyToConfigsMap.put(SPARK_CONFIGS, sparkConfigsMap);
        keyToConfigsMap.put(YARN_CONFIGS, yarnConfigsMap);
        for (Map.Entry<String, Map<String, String>> entry : keyToConfigsMap.entrySet()) {
            Map<String, String> configsMap = entry.getValue();
            if (!configsMap.isEmpty()) {
                List<String> configs = Lists.newArrayList();
                for (Map.Entry<String, String> argEntry : configsMap.entrySet()) {
                    configs.add(argEntry.getKey() + SparkEtlCluster.EQUAL_SEPARATOR + argEntry.getValue());
                }
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(),
                                                 StringUtils.join(configs, SparkEtlCluster.SEMICOLON_SEPARATOR)));
            }
        }
    }

    private Map<String, String> getSparkArgsMap(Map<String, String> properties, String sparkArgsKey)
            throws DdlException {
        Map<String, String> sparkArgsMap = getConfigsMap(properties, sparkArgsKey);
        for (Map.Entry<String, String> entry : sparkArgsMap.entrySet()) {
            if (!entry.getKey().startsWith(SPARK_ARG_PREFIX)) {
                throw new DdlException(sparkArgsKey + " format error, use '--key1=value1,value2;--key2=value3'");
            }
        }
        return sparkArgsMap;
    }

    private Map<String, String> getSparkConfigsMap(Map<String, String> properties, String sparkConfigsKey)
            throws DdlException {
        Map<String, String> sparkConfigsMap = getConfigsMap(properties, sparkConfigsKey);
        for (Map.Entry<String, String> entry : sparkConfigsMap.entrySet()) {
            if (!entry.getKey().startsWith(SPARK_CONFIG_PREFIX)) {
                throw new DdlException(sparkConfigsKey + " format error, use 'spark.key1=value1;spark.key2=value2'");
            }
        }
        return sparkConfigsMap;
    }

    private Map<String, String> getYarnConfigsMap(Map<String, String> properties, String yarnConfigsKey) throws DdlException {
        Map<String, String> yarnConfigsMap = getConfigsMap(properties, yarnConfigsKey);
        /*
        // if deploy machines do not set HADOOP_CONF_DIR env, we should set these configs blow
        if ((!yarnConfigsMap.containsKey(YARN_RESOURCE_MANAGER_ADDRESS) || !yarnConfigsMap.containsKey(FS_DEFAULT_FS))
                && isYarnMaster()) {
            throw new DdlException("Missing " + yarnConfigsKey + "(" + YARN_RESOURCE_MANAGER_ADDRESS + " and " + FS_DEFAULT_FS
                                           + ") in yarn master");
        }
        */
        return yarnConfigsMap;
    }

    private Map<String, String> getConfigsMap(Map<String, String> properties, String key) throws DdlException {
        Map<String, String> configsMap = Maps.newHashMap();

        String configsStr = properties.get(key);
        if (Strings.isNullOrEmpty(configsStr)) {
            return configsMap;
        }

        // --jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb
        // yarn.resourcemanager.address=host:port;fs.defaultFS=hdfs://host:port
        // spark.driver.memory=1g;spark.executor.memory=1g
        String[] configs = configsStr.split(SEMICOLON_SEPARATOR);
        for (String configStr : configs) {
            if (configStr.trim().isEmpty()) {
                continue;
            }

            String[] configArr = configStr.trim().split(EQUAL_SEPARATOR);
            if (configArr.length != 2 || configArr[0].isEmpty() || configArr[1].isEmpty()) {
                throw new DdlException(key + " format error, use 'key1=value1;key2=value2'");
            }
             configsMap.put(configArr[0], configArr[1]);
        }
        return  configsMap;
    }
}

