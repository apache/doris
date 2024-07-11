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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.SetUserPropertyVar;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/*
 * UserProperty contains properties set for a user
 * This user is just qualified by cluster name, not host which it connected from.
 *
 * If UserProperty and SessionVeriable have the same name, UserProperty has a higher priority than SessionVeriable.
 * This usually means that the cluster administrator force user restrictions.
 * Users cannot modify these SessionVeriables with the same name.
 */
public class UserProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(UserProperty.class);
    // advanced properties
    public static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    public static final String PROP_MAX_QUERY_INSTANCES = "max_query_instances";
    public static final String PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String PROP_RESOURCE_TAGS = "resource_tags";
    public static final String PROP_RESOURCE = "resource";
    public static final String PROP_SQL_BLOCK_RULES = "sql_block_rules";
    public static final String PROP_CPU_RESOURCE_LIMIT = "cpu_resource_limit";
    public static final String PROP_EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String PROP_USER_QUERY_TIMEOUT = "query_timeout";

    public static final String PROP_USER_INSERT_TIMEOUT = "insert_timeout";
    // advanced properties end

    public static final String PROP_LOAD_CLUSTER = "load_cluster";
    public static final String PROP_QUOTA = "quota";
    public static final String PROP_DEFAULT_LOAD_CLUSTER = "default_load_cluster";

    public static final String PROP_WORKLOAD_GROUP = "default_workload_group";

    public static final String DEFAULT_CLOUD_CLUSTER = "default_cloud_cluster";

    // for system user
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

    @SerializedName(value = "qu", alternate = {"qualifiedUser"})
    private String qualifiedUser;

    @SerializedName(value = "cp", alternate = {"commonProperties"})
    private CommonUserProperties commonProperties = new CommonUserProperties();

    // load cluster
    @SerializedName(value = "dlc", alternate = {"defaultLoadCluster"})
    private String defaultLoadCluster = null;

    @SerializedName(value = "cdc", alternate = {"clusterToDppConfig"})
    private Map<String, DppConfig> clusterToDppConfig = Maps.newHashMap();

    @SerializedName(value = "dcc", alternate = {"defaultCloudCluster"})
    private String defaultCloudCluster = null;

    /*
     *  We keep white list here to save Baidu domain name (BNS) or DNS as white list.
     *  Each frontend will periodically resolve the domain name to ip, and update the privilege table.
     *  We never persist the resolved IPs.
     */
    @Deprecated
    private WhiteList whiteList = new WhiteList();

    public static final Set<Tag> INVALID_RESOURCE_TAGS;

    static {
        INVALID_RESOURCE_TAGS = Sets.newHashSet();
        INVALID_RESOURCE_TAGS.add(Tag.INVALID_TAG);
    }

    static {
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_MAX_USER_CONNECTIONS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE + ".", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + "."
                + DppConfig.PRIORITY + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_MAX_QUERY_INSTANCES + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM + "$",
                Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_SQL_BLOCK_RULES + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_CPU_RESOURCE_LIMIT + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE_TAGS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_EXEC_MEM_LIMIT + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_USER_QUERY_TIMEOUT + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_USER_INSERT_TIMEOUT + "$", Pattern.CASE_INSENSITIVE));

        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_QUOTA + ".", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_DEFAULT_LOAD_CLUSTER + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + ".",
                Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_WORKLOAD_GROUP + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + DEFAULT_CLOUD_CLUSTER + "$", Pattern.CASE_INSENSITIVE));
    }

    public UserProperty() {
    }

    public UserProperty(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public long getMaxConn() {
        return this.commonProperties.getMaxConn();
    }

    public int getQueryTimeout() {
        return this.commonProperties.getQueryTimeout();
    }

    public int getInsertTimeout() {
        return this.commonProperties.getInsertTimeout();
    }

    public long getMaxQueryInstances() {
        return commonProperties.getMaxQueryInstances(); // maxQueryInstances;
    }

    public int getParallelFragmentExecInstanceNum() {
        return commonProperties.getParallelFragmentExecInstanceNum();
    }

    public String[] getSqlBlockRules() {
        return commonProperties.getSqlBlockRulesSplit();
    }

    public int getCpuResourceLimit() {
        return commonProperties.getCpuResourceLimit();
    }

    public String getWorkloadGroup() {
        return commonProperties.getWorkloadGroup();
    }

    @Deprecated
    public WhiteList getWhiteList() {
        return whiteList;
    }

    public Set<Tag> getCopiedResourceTags() {
        return Sets.newHashSet(this.commonProperties.getResourceTags());
    }

    public long getExecMemLimit() {
        return commonProperties.getExecMemLimit();
    }

    public void update(List<Pair<String, String>> properties) throws UserException {
        update(properties, false);
    }

    public void update(List<Pair<String, String>> properties, boolean isReplay) throws UserException {
        // copy
        long newMaxConn = this.commonProperties.getMaxConn();
        long newMaxQueryInstances = this.commonProperties.getMaxQueryInstances();
        int newParallelFragmentExecInstanceNum = this.commonProperties.getParallelFragmentExecInstanceNum();
        String sqlBlockRules = this.commonProperties.getSqlBlockRules();
        int cpuResourceLimit = this.commonProperties.getCpuResourceLimit();
        Set<Tag> resourceTags = this.commonProperties.getResourceTags();
        long execMemLimit = this.commonProperties.getExecMemLimit();
        int queryTimeout = this.commonProperties.getQueryTimeout();
        int insertTimeout = this.commonProperties.getInsertTimeout();
        String workloadGroup = this.commonProperties.getWorkloadGroup();

        String newDefaultLoadCluster = defaultLoadCluster;
        String newDefaultCloudCluster = defaultCloudCluster;
        Map<String, DppConfig> newDppConfigs = Maps.newHashMap(clusterToDppConfig);

        // update
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            String[] keyArr = key.split("\\" + SetUserPropertyVar.DOT_SEPARATOR);
            if (keyArr[0].equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                // set property "max_user_connections" = "1000"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " format error");
                }

                try {
                    newMaxConn = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not number");
                }

                if (newMaxConn <= 0 || newMaxConn > 10000) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, must between 1 and 10000");
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_LOAD_CLUSTER)) {
                updateLoadCluster(keyArr, value, newDppConfigs);
            } else if (keyArr[0].equalsIgnoreCase(PROP_DEFAULT_LOAD_CLUSTER)) {
                // set property "default_load_cluster" = "cluster1"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_DEFAULT_LOAD_CLUSTER + " format error");
                }
                if (value != null && !newDppConfigs.containsKey(value)) {
                    throw new DdlException("Load cluster[" + value + "] does not exist");
                }

                newDefaultLoadCluster = value;
            }  else if (keyArr[0].equalsIgnoreCase(DEFAULT_CLOUD_CLUSTER)) {
                // set property "DEFAULT_CLOUD_CLUSTER" = "cluster1"
                if (keyArr.length != 1) {
                    throw new DdlException(DEFAULT_CLOUD_CLUSTER + " format error");
                }
                if (value == null) {
                    value = "";
                }
                newDefaultCloudCluster = value;
            } else if (keyArr[0].equalsIgnoreCase(PROP_MAX_QUERY_INSTANCES)) {
                // set property "max_query_instances" = "1000"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_MAX_QUERY_INSTANCES + " format error");
                }

                try {
                    newMaxQueryInstances = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_MAX_QUERY_INSTANCES + " is not number");
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM)) {
                // set property "parallel_fragment_exec_instance_num" = "16"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM + " format error");
                }

                try {
                    newParallelFragmentExecInstanceNum = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM + " is not number");
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_SQL_BLOCK_RULES)) {
                // set property "sql_block_rules" = "test_rule1,test_rule2"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_SQL_BLOCK_RULES + " format error");
                }
                sqlBlockRules = value;
            } else if (keyArr[0].equalsIgnoreCase(PROP_CPU_RESOURCE_LIMIT)) {
                // set property "cpu_resource_limit" = "2";
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_CPU_RESOURCE_LIMIT + " format error");
                }
                int limit = -1;
                try {
                    limit = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(key + " is not number");
                }

                // -1 means unlimited
                if (limit <= 0 && limit != -1) {
                    throw new DdlException(key + " is not valid. Should not larger than 0 or equal to -1");
                }

                cpuResourceLimit = limit;
            } else if (keyArr[0].equalsIgnoreCase(PROP_RESOURCE_TAGS)) {
                if (keyArr.length != 2) {
                    throw new DdlException(PROP_RESOURCE_TAGS + " format error");
                }
                if (!keyArr[1].equals(Tag.TYPE_LOCATION)) {
                    throw new DdlException("Only support location tag now");
                }

                if (Strings.isNullOrEmpty(value)) {
                    // This is for compatibility. empty value means to unset the resource tag property.
                    // So that user will have permission to query all tags.
                    resourceTags = Sets.newHashSet();
                } else {
                    try {
                        resourceTags = parseLocationResoureTags(value);
                    } catch (NumberFormatException e) {
                        throw new DdlException(PROP_RESOURCE_TAGS + " parse failed: " + e.getMessage());
                    }
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_EXEC_MEM_LIMIT)) {
                // set property "exec_mem_limit" = "2147483648";
                execMemLimit = getLongProperty(key, value, keyArr, PROP_EXEC_MEM_LIMIT);
            } else if (keyArr[0].equalsIgnoreCase(PROP_USER_QUERY_TIMEOUT)) {
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_USER_QUERY_TIMEOUT + " format error");
                }
                try {
                    queryTimeout = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_USER_QUERY_TIMEOUT + " is not number");
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_USER_INSERT_TIMEOUT)) {
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_USER_INSERT_TIMEOUT + " format error");
                }
                try {
                    insertTimeout = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_USER_INSERT_TIMEOUT + " is not number");
                }
            } else if (keyArr[0].equalsIgnoreCase(PROP_WORKLOAD_GROUP)) {
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_WORKLOAD_GROUP + " format error");
                }
                boolean ret = Env.getCurrentEnv().getWorkloadGroupMgr().isWorkloadGroupExists(value);
                if (!ret) {
                    throw new DdlException("workload group " + value + " not exists");
                }
                workloadGroup = value;
            } else {
                if (isReplay) {
                    // After using SET PROPERTY to modify the user property, if FE rolls back to a version without
                    // this property, `Unknown user property` error will be reported when replay EditLog,
                    // just ignore it.
                    LOG.warn("Unknown user property(" + key + "), maybe FE rolled back version, Ignore it");
                } else {
                    throw new DdlException("Unknown user property(" + key + ")");
                }
            }
        }

        // set
        this.commonProperties.setMaxConn(newMaxConn);
        this.commonProperties.setMaxQueryInstances(newMaxQueryInstances);
        this.commonProperties.setParallelFragmentExecInstanceNum(newParallelFragmentExecInstanceNum);
        this.commonProperties.setSqlBlockRules(sqlBlockRules);
        this.commonProperties.setCpuResourceLimit(cpuResourceLimit);
        this.commonProperties.setResourceTags(resourceTags);
        this.commonProperties.setExecMemLimit(execMemLimit);
        this.commonProperties.setQueryTimeout(queryTimeout);
        this.commonProperties.setInsertTimeout(insertTimeout);
        this.commonProperties.setWorkloadGroup(workloadGroup);
        if (newDppConfigs.containsKey(newDefaultLoadCluster)) {
            defaultLoadCluster = newDefaultLoadCluster;
        } else {
            defaultLoadCluster = null;
        }
        clusterToDppConfig = newDppConfigs;
        defaultCloudCluster = newDefaultCloudCluster;
    }

    private long getLongProperty(String key, String value, String[] keyArr, String propName) throws DdlException {
        // eg: set property "load_mem_limit" = "2147483648";
        if (keyArr.length != 1) {
            throw new DdlException(propName + " format error");
        }
        long limit = -1;
        try {
            limit = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new DdlException(key + " is not number");
        }

        // -1 means unlimited
        if (limit <= 0 && limit != -1) {
            throw new DdlException(key + " is not valid. Should not larger than 0 or equal to -1");
        }
        return limit;
    }

    private Set<Tag> parseLocationResoureTags(String value) throws AnalysisException {
        Set<Tag> tags = Sets.newHashSet();
        String[] parts = value.replaceAll(" ", "").split(",");
        for (String part : parts) {
            Tag tag = Tag.create(Tag.TYPE_LOCATION, part);
            tags.add(tag);
        }
        return tags;
    }

    private void updateLoadCluster(String[] keyArr, String value, Map<String, DppConfig> newDppConfigs)
            throws DdlException {
        if (keyArr.length == 1 && Strings.isNullOrEmpty(value)) {
            // set property "load_cluster" = '';
            newDppConfigs.clear();
        } else if (keyArr.length == 2 && Strings.isNullOrEmpty(value)) {
            // set property "load_cluster.cluster1" = ''
            String cluster = keyArr[1];
            newDppConfigs.remove(cluster);
        } else if (keyArr.length == 3 && Strings.isNullOrEmpty(value)) {
            // set property "load_cluster.cluster1.xxx" = ''
            String cluster = keyArr[1];
            if (!newDppConfigs.containsKey(cluster)) {
                throw new DdlException("Load cluster[" + value + "] does not exist");
            }

            try {
                newDppConfigs.get(cluster).resetConfigByKey(keyArr[2]);
            } catch (LoadException e) {
                throw new DdlException(e.getMessage());
            }
        } else if (keyArr.length == 3 && value != null) {
            // set property "load_cluster.cluster1.xxx" = "xxx"
            String cluster = keyArr[1];
            Map<String, String> configMap = Maps.newHashMap();
            configMap.put(keyArr[2], value);

            try {
                DppConfig newDppConfig = DppConfig.create(configMap);

                if (newDppConfigs.containsKey(cluster)) {
                    newDppConfigs.get(cluster).update(newDppConfig, true);
                } else {
                    newDppConfigs.put(cluster, newDppConfig);
                }
            } catch (LoadException e) {
                throw new DdlException(e.getMessage());
            }
        } else {
            throw new DdlException(PROP_LOAD_CLUSTER + " format error");
        }
    }

    public String getDefaultCloudCluster() {
        return defaultCloudCluster;
    }

    public String getDefaultLoadCluster() {
        return defaultLoadCluster;
    }

    public Pair<String, DppConfig> getLoadClusterInfo(String cluster) {
        String tmpCluster = cluster;
        if (tmpCluster == null) {
            tmpCluster = defaultLoadCluster;
        }

        DppConfig dppConfig = null;
        if (tmpCluster != null) {
            dppConfig = clusterToDppConfig.get(tmpCluster);
            if (dppConfig != null) {
                dppConfig = dppConfig.getCopiedDppConfig();
            }
        }

        return Pair.of(tmpCluster, dppConfig);
    }

    public List<List<String>> fetchProperty() {
        List<List<String>> result = Lists.newArrayList();
        String dot = SetUserPropertyVar.DOT_SEPARATOR;

        // max user connections
        result.add(Lists.newArrayList(PROP_MAX_USER_CONNECTIONS, String.valueOf(commonProperties.getMaxConn())));

        // max query instance
        result.add(Lists.newArrayList(PROP_MAX_QUERY_INSTANCES,
                String.valueOf(commonProperties.getMaxQueryInstances())));

        // parallel fragment exec instance num
        result.add(Lists.newArrayList(PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM,
                String.valueOf(commonProperties.getParallelFragmentExecInstanceNum())));

        // sql block rules
        result.add(Lists.newArrayList(PROP_SQL_BLOCK_RULES, commonProperties.getSqlBlockRules()));

        // cpu resource limit
        result.add(Lists.newArrayList(PROP_CPU_RESOURCE_LIMIT, String.valueOf(commonProperties.getCpuResourceLimit())));

        // exec mem limit
        result.add(Lists.newArrayList(PROP_EXEC_MEM_LIMIT, String.valueOf(commonProperties.getExecMemLimit())));

        // query timeout
        result.add(Lists.newArrayList(PROP_USER_QUERY_TIMEOUT, String.valueOf(commonProperties.getQueryTimeout())));

        // insert timeout
        result.add(Lists.newArrayList(PROP_USER_INSERT_TIMEOUT, String.valueOf(commonProperties.getInsertTimeout())));

        // resource tag
        result.add(Lists.newArrayList(PROP_RESOURCE_TAGS, Joiner.on(", ").join(commonProperties.getResourceTags())));

        result.add(Lists.newArrayList(PROP_WORKLOAD_GROUP, String.valueOf(commonProperties.getWorkloadGroup())));

        // load cluster
        if (defaultLoadCluster != null) {
            result.add(Lists.newArrayList(PROP_DEFAULT_LOAD_CLUSTER, defaultLoadCluster));
        } else {
            result.add(Lists.newArrayList(PROP_DEFAULT_LOAD_CLUSTER, ""));
        }

        // default cloud cluster
        if (defaultCloudCluster != null) {
            result.add(Lists.newArrayList(DEFAULT_CLOUD_CLUSTER, defaultCloudCluster));
        } else {
            result.add(Lists.newArrayList(DEFAULT_CLOUD_CLUSTER, ""));
        }

        for (Map.Entry<String, DppConfig> entry : clusterToDppConfig.entrySet()) {
            String cluster = entry.getKey();
            DppConfig dppConfig = entry.getValue();
            String clusterPrefix = PROP_LOAD_CLUSTER + dot + cluster + dot;

            // palo path
            if (dppConfig.getPaloPath() != null) {
                result.add(Lists.newArrayList(clusterPrefix + DppConfig.getPaloPathKey(), dppConfig.getPaloPath()));
            }

            // http port
            result.add(Lists.newArrayList(clusterPrefix + DppConfig.getHttpPortKey(),
                    String.valueOf(dppConfig.getHttpPort())));

            // hadoop configs
            if (dppConfig.getHadoopConfigs() != null) {
                List<String> configs = Lists.newArrayList();
                for (Map.Entry<String, String> configEntry : dppConfig.getHadoopConfigs().entrySet()) {
                    configs.add(String.format("%s=%s", configEntry.getKey(), configEntry.getValue()));
                }
                result.add(Lists.newArrayList(clusterPrefix + DppConfig.getHadoopConfigsKey(),
                        StringUtils.join(configs, ";")));
            }

            // priority
            result.add(Lists.newArrayList(clusterPrefix + DppConfig.getPriorityKey(),
                    String.valueOf(dppConfig.getPriority())));
        }

        // sort
        Collections.sort(result, new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return o1.get(0).compareTo(o2.get(0));
            }
        });

        return result;
    }

    public static UserProperty read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_130) {
            UserProperty userProperty = new UserProperty();
            userProperty.readFields(in);
            return userProperty;
        }
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UserProperty.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        qualifiedUser = Text.readString(in);
        // should be removed after version 3.0
        qualifiedUser = ClusterNamespace.getNameFromFullName(qualifiedUser);

        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_100) {
            long maxConn = in.readLong();
            this.commonProperties.setMaxConn(maxConn);
        }

        // call UserResource.readIn(out) to make sure that FE can rollback.
        UserResource.readIn(in);

        // load cluster
        if (in.readBoolean()) {
            defaultLoadCluster = Text.readString(in);
        }

        int clusterNum = in.readInt();
        for (int i = 0; i < clusterNum; ++i) {
            String cluster = Text.readString(in);
            DppConfig dppConfig = new DppConfig();
            dppConfig.readFields(in);
            clusterToDppConfig.put(cluster, dppConfig);
        }

        if (Config.isCloudMode()) {
            if (in.readBoolean()) {
                defaultCloudCluster = Text.readString(in);
            }
        }

        // whiteList
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_116) {
            whiteList.readFields(in);
        } else {
            whiteList = new WhiteList();
        }

        // common properties
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_100) {
            this.commonProperties = CommonUserProperties.read(in);
        }
    }
}
