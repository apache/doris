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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.load.loadv2.SparkRepository;
import org.apache.doris.load.loadv2.SparkYarnConfigFiles;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private static final Logger LOG = LogManager.getLogger(SparkResource.class);

    private static final String SPARK_MASTER = "spark.master";
    private static final String SPARK_SUBMIT_DEPLOY_MODE = "spark.submit.deployMode";
    private static final String WORKING_DIR = "working_dir";
    private static final String BROKER = "broker";
    private static final String YARN_MASTER = "yarn";
    private static final String SPARK_CONFIG_PREFIX = "spark.";
    private static final String BROKER_PROPERTY_PREFIX = "broker.";
    private static final String ENV_PREFIX = "env.";
    // spark uses hadoop configs in the form of spark.hadoop.*
    private static final String SPARK_HADOOP_CONFIG_PREFIX = "spark.hadoop.";
    private static final String SPARK_YARN_RESOURCE_MANAGER_ADDRESS = "spark.hadoop.yarn.resourcemanager.address";
    private static final String SPARK_FS_DEFAULT_FS = "spark.hadoop.fs.defaultFS";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.address";
    private static final String SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED = "spark.hadoop.yarn.resourcemanager.ha.enabled";
    private static final String SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS = "spark.hadoop.yarn.resourcemanager.ha.rm-ids";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS_FOMART = "spark.hadoop.yarn.resourcemanager.address.%s";
    private static final String YARN_RESOURCE_MANAGER_HOSTNAME_FORMAT = "spark.hadoop.yarn.resourcemanager.hostname.%s";

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
    @SerializedName(value = "envConfigs")
    private Map<String, String> envConfigs;


    public SparkResource() {
        super();
    }

    public SparkResource(String name) {
        this(name, Maps.newHashMap(), null, null, Maps.newHashMap(), Maps.newHashMap());
    }

    // "public" for testing
    public SparkResource(String name, Map<String, String> sparkConfigs, String workingDir, String broker,
                         Map<String, String> brokerProperties, Map<String, String> envConfigs) {
        super(name, ResourceType.SPARK);
        this.sparkConfigs = sparkConfigs;
        this.workingDir = workingDir;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
        this.envConfigs = envConfigs;
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

    public Map<String, String> getEnvConfigsWithoutPrefix() {
        return Stream.concat(
                        getSystemEnvConfigs().entrySet().stream(),
                        Optional.ofNullable(envConfigs).orElse(Collections.emptyMap()).entrySet().stream()
                )
                .filter(entry -> entry.getKey().startsWith(ENV_PREFIX))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(ENV_PREFIX.length()),
                        Entry::getValue,
                        (oldValue, newValue) -> newValue
                ));
    }

    public Map<String, String> getSystemEnvConfigs() {
        return System.getenv();
    }

    public Pair<String, String> getYarnResourcemanagerAddressPair() {
        return Pair.of(YARN_RESOURCE_MANAGER_ADDRESS, sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_ADDRESS));
    }

    public SparkResource getCopiedResource() {
        return new SparkResource(name, Maps.newHashMap(sparkConfigs), workingDir, broker, brokerProperties, envConfigs);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        Map<String, String> copiedProperties = Maps.newHashMap(sparkConfigs);
        return copiedProperties;
    }

    /**
     * Each SparkResource has and only has one SparkRepository.
     * This method get the remote archive which matches the dpp version from remote repository
     */
    public synchronized SparkRepository.SparkArchive prepareArchive() throws LoadException {
        String remoteRepositoryPath = workingDir + "/" + Env.getCurrentEnv().getClusterId()
                + "/" + SparkRepository.REPOSITORY_DIR + name;
        BrokerDesc brokerDesc = new BrokerDesc(broker, getBrokerPropertiesWithoutPrefix());
        SparkRepository repository = new SparkRepository(remoteRepositoryPath, brokerDesc);
        // This checks and uploads the remote archive.
        repository.prepare();
        SparkRepository.SparkArchive archive = repository.getCurrentArchive();
        // Normally, an archive should contain a DPP library and a SPARK library
        Preconditions.checkState(archive.libraries.size() == 2);
        SparkRepository.SparkLibrary dppLibrary = archive.getDppLibrary();
        SparkRepository.SparkLibrary spark2xLibrary = archive.getSpark2xLibrary();
        if (dppLibrary == null || spark2xLibrary == null) {
            throw new LoadException("failed to get libraries from remote archive");
        }
        return archive;
    }

    // Each SparkResource has and only has one yarn config to run yarn command
    // This method will write all the configuration start with "spark.hadoop." into config files in a specific directory
    public synchronized String prepareYarnConfig() throws LoadException {
        SparkYarnConfigFiles yarnConfigFiles = new SparkYarnConfigFiles(name, getSparkHadoopConfig(sparkConfigs));
        yarnConfigFiles.prepare();
        return yarnConfigFiles.getConfigDir();
    }

    public String getYarnClientPath() throws LoadException {
        String yarnClientPath = Config.yarn_client_path;
        File file = new File(yarnClientPath);
        if (!file.exists() || !file.isFile()) {
            throw new LoadException("yarn client does not exist in path: " + yarnClientPath);
        }
        return yarnClientPath;
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

        // update properties
        updateProperties(properties);
    }

    private void updateProperties(Map<String, String> properties) throws DdlException {
        // update spark configs
        if (properties.containsKey(SPARK_MASTER)) {
            throw new DdlException("Cannot change spark master");
        }
        sparkConfigs.putAll(getSparkConfig(properties));

        // update working dir and broker
        if (properties.containsKey(WORKING_DIR)) {
            workingDir = properties.get(WORKING_DIR);
        }
        if (properties.containsKey(BROKER)) {
            broker = properties.get(BROKER);
        }
        brokerProperties.putAll(getBrokerProperties(properties));
        Map<String, String> env = getEnvConfig(properties);
        if (env.size() > 0) {
            if (envConfigs == null) {
                envConfigs = env;
            } else {
                envConfigs.putAll(env);
            }
        }
        LOG.info("updateProperties,{},{}", properties, envConfigs);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        // get spark configs
        sparkConfigs = getSparkConfig(properties);
        envConfigs = getEnvConfig(properties);
        LOG.info("setProperties,{},{}", properties, envConfigs);
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
        if (isYarnMaster()) {
            if (!sparkConfigs.containsKey(SPARK_FS_DEFAULT_FS)) {
                throw new DdlException("Missing (" + SPARK_FS_DEFAULT_FS + ") in yarn master");
            }

            String haEnabled = sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED);
            if (StringUtils.isNotEmpty(haEnabled) && "true".equals(haEnabled)) {
                if (StringUtils.isEmpty(SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS)) {
                    throw new DdlException("Missing (" + SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS + ") in yarn master, "
                        + "when " + SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED + "=true.");
                }

                String[] haIds = sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS).split(",");
                for (String haId : haIds) {
                    String addressKey = String.format(YARN_RESOURCE_MANAGER_ADDRESS_FOMART, haId);
                    String hostnameKey = String.format(YARN_RESOURCE_MANAGER_HOSTNAME_FORMAT, haId);
                    if (!sparkConfigs.containsKey(addressKey) && !sparkConfigs.containsKey(hostnameKey)) {
                        throw new DdlException("Missing " + addressKey + " or " + hostnameKey + " in yarn master, "
                            + "when " + SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED + "=true.");
                    }
                }
            } else if (!sparkConfigs.containsKey(SPARK_YARN_RESOURCE_MANAGER_ADDRESS)) {
                throw new DdlException("Missing (" + SPARK_YARN_RESOURCE_MANAGER_ADDRESS + ") in yarn master, "
                    + "or not turned on ha.");
            }
        }

        // check working dir and broker
        workingDir = properties.get(WORKING_DIR);
        broker = properties.get(BROKER);
        if ((workingDir == null && broker != null) || (workingDir != null && broker == null)) {
            throw new DdlException("working_dir and broker should be assigned at the same time");
        }
        // check broker exist
        if (broker != null && !Env.getCurrentEnv().getBrokerMgr().containsBroker(broker)) {
            throw new DdlException("Unknown broker name(" + broker + ")");
        }
        brokerProperties = getBrokerProperties(properties);
    }

    private Map<String, String> getSparkConfig(Map<String, String> properties) {
        Map<String, String> sparkConfig = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SPARK_CONFIG_PREFIX)) {
                sparkConfig.put(entry.getKey(), entry.getValue());
            }
        }
        return sparkConfig;
    }

    private Map<String, String> getEnvConfig(Map<String, String> properties) {
        Map<String, String> envConfig = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(ENV_PREFIX)) {
                envConfig.put(entry.getKey(), entry.getValue());
            }
        }
        return envConfig;
    }

    private Map<String, String> getSparkHadoopConfig(Map<String, String> properties) {
        Map<String, String> sparkConfig = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SPARK_HADOOP_CONFIG_PREFIX)) {
                sparkConfig.put(entry.getKey(), entry.getValue());
            }
        }
        return sparkConfig;
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
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        updateProperties(properties);
        super.modifyProperties(properties);
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.keySet().removeAll(getSparkConfig(properties).keySet());
        copiedProperties.keySet().removeAll(getBrokerProperties(properties).keySet());
        copiedProperties.remove(BROKER);
        copiedProperties.remove(WORKING_DIR);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown spark resource properties: " + copiedProperties);
        }
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
        if (envConfigs != null) {
            for (Map.Entry<String, String> entry : envConfigs.entrySet()) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }
}
