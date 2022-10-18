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
import org.apache.doris.catalog.ResourceGroup;
import org.apache.doris.catalog.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/*
 * UserProperty contains properties set for a user
 * This user is just qualified by cluster name, not host which it connected from.
 */
public class UserProperty implements Writable {
    // advanced properties
    private static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    private static final String PROP_MAX_QUERY_INSTANCES = "max_query_instances";
    private static final String PROP_RESOURCE_TAGS = "resource_tags";
    private static final String PROP_RESOURCE = "resource";
    private static final String PROP_SQL_BLOCK_RULES = "sql_block_rules";
    private static final String PROP_CPU_RESOURCE_LIMIT = "cpu_resource_limit";
    private static final String PROP_EXEC_MEM_LIMIT = "exec_mem_limit";
    private static final String PROP_USER_QUERY_TIMEOUT = "query_timeout";
    // advanced properties end

    private static final String PROP_LOAD_CLUSTER = "load_cluster";
    private static final String PROP_QUOTA = "quota";
    private static final String PROP_DEFAULT_LOAD_CLUSTER = "default_load_cluster";

    // for system user
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

    private String qualifiedUser;

    private CommonUserProperties commonProperties = new CommonUserProperties();

    // Resource belong to this user.
    private UserResource resource = new UserResource(1000);
    // load cluster
    private String defaultLoadCluster = null;
    private Map<String, DppConfig> clusterToDppConfig = Maps.newHashMap();

    /*
     *  We keep white list here to save Baidu domain name (BNS) or DNS as white list.
     *  Each frontend will periodically resolve the domain name to ip, and update the privilege table.
     *  We never persist the resolved IPs.
     */
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
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_SQL_BLOCK_RULES + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_CPU_RESOURCE_LIMIT + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE_TAGS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_EXEC_MEM_LIMIT + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_USER_QUERY_TIMEOUT + "$", Pattern.CASE_INSENSITIVE));

        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_QUOTA + ".", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_DEFAULT_LOAD_CLUSTER + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + ".",
                Pattern.CASE_INSENSITIVE));
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

    public long getQueryTimeout() {
        return this.commonProperties.getQueryTimeout();
    }

    public long getMaxQueryInstances() {
        return commonProperties.getMaxQueryInstances(); // maxQueryInstances;
    }

    public String[] getSqlBlockRules() {
        return commonProperties.getSqlBlockRulesSplit();
    }

    public int getCpuResourceLimit() {
        return commonProperties.getCpuResourceLimit();
    }

    public WhiteList getWhiteList() {
        return whiteList;
    }

    public Set<Tag> getCopiedResourceTags() {
        return Sets.newHashSet(this.commonProperties.getResourceTags());
    }

    public long getExecMemLimit() {
        return commonProperties.getExecMemLimit();
    }

    public void setPasswordForDomain(String domain, byte[] password, boolean errOnExist) throws DdlException {
        if (errOnExist && whiteList.containsDomain(domain)) {
            throw new DdlException("Domain " + domain + " of user " + qualifiedUser + " already exists");
        }

        if (password != null) {
            whiteList.setPassword(domain, password);
        }
    }

    public void removeDomain(String domain) {
        whiteList.removeDomain(domain);
    }

    public void update(List<Pair<String, String>> properties) throws UserException {
        // copy
        long newMaxConn = this.commonProperties.getMaxConn();
        long newMaxQueryInstances = this.commonProperties.getMaxQueryInstances();
        String sqlBlockRules = this.commonProperties.getSqlBlockRules();
        int cpuResourceLimit = this.commonProperties.getCpuResourceLimit();
        Set<Tag> resourceTags = this.commonProperties.getResourceTags();
        long execMemLimit = this.commonProperties.getExecMemLimit();
        long queryTimeout = this.commonProperties.getQueryTimeout();

        UserResource newResource = resource.getCopiedUserResource();
        String newDefaultLoadCluster = defaultLoadCluster;
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
            } else if (keyArr[0].equalsIgnoreCase(PROP_RESOURCE)) {
                // set property "resource.cpu_share" = "100"
                if (keyArr.length != 2) {
                    throw new DdlException(PROP_RESOURCE + " format error");
                }

                int resource = 0;
                try {
                    resource = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(key + " is not number");
                }

                if (resource <= 0) {
                    throw new DdlException(key + " is not valid");
                }

                newResource.updateResource(keyArr[1], resource);
            } else if (keyArr[0].equalsIgnoreCase(PROP_QUOTA)) {
                // set property "quota.normal" = "100"
                if (keyArr.length != 2) {
                    throw new DdlException(PROP_QUOTA + " format error");
                }

                int quota = 0;
                try {
                    quota = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(key + " is not number");
                }

                if (quota <= 0) {
                    throw new DdlException(key + " is not valid");
                }

                newResource.updateGroupShare(keyArr[1], quota);
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
            } else if (keyArr[0].equalsIgnoreCase(PROP_SQL_BLOCK_RULES)) {
                // set property "sql_block_rules" = "test_rule1,test_rule2"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_SQL_BLOCK_RULES + " format error");
                }

                // check if sql_block_rule has already exist
                for (String ruleName : value.replaceAll(" ", "").split(",")) {
                    if (!ruleName.equals("") && !Env.getCurrentEnv().getSqlBlockRuleMgr().existRule(ruleName)) {
                        throw new DdlException("the sql block rule " + ruleName + " not exist");
                    }
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
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " format error");
                }
                try {
                    queryTimeout = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_USER_QUERY_TIMEOUT + " is not number");
                }
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }

        // set
        this.commonProperties.setMaxConn(newMaxConn);
        this.commonProperties.setMaxQueryInstances(newMaxQueryInstances);
        this.commonProperties.setSqlBlockRules(sqlBlockRules);
        this.commonProperties.setCpuResourceLimit(cpuResourceLimit);
        this.commonProperties.setResourceTags(resourceTags);
        this.commonProperties.setExecMemLimit(execMemLimit);
        this.commonProperties.setQueryTimeout(queryTimeout);
        resource = newResource;
        if (newDppConfigs.containsKey(newDefaultLoadCluster)) {
            defaultLoadCluster = newDefaultLoadCluster;
        } else {
            defaultLoadCluster = null;
        }
        clusterToDppConfig = newDppConfigs;
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

    public UserResource getResource() {
        return resource;
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

        // sql block rules
        result.add(Lists.newArrayList(PROP_SQL_BLOCK_RULES, commonProperties.getSqlBlockRules()));

        // cpu resource limit
        result.add(Lists.newArrayList(PROP_CPU_RESOURCE_LIMIT, String.valueOf(commonProperties.getCpuResourceLimit())));

        // exec mem limit
        result.add(Lists.newArrayList(PROP_EXEC_MEM_LIMIT, String.valueOf(commonProperties.getExecMemLimit())));

        // timeout limit
        result.add(Lists.newArrayList(PROP_USER_QUERY_TIMEOUT, String.valueOf(commonProperties.getQueryTimeout())));

        // resource tag
        result.add(Lists.newArrayList(PROP_RESOURCE_TAGS, Joiner.on(", ").join(commonProperties.getResourceTags())));

        // resource
        ResourceGroup group = resource.getResource();
        for (Map.Entry<ResourceType, Integer> entry : group.getQuotaMap().entrySet()) {
            result.add(Lists.newArrayList(PROP_RESOURCE + dot + entry.getKey().getDesc().toLowerCase(),
                    entry.getValue().toString()));
        }

        // quota
        Map<String, AtomicInteger> groups = resource.getShareByGroup();
        for (Map.Entry<String, AtomicInteger> entry : groups.entrySet()) {
            result.add(Lists.newArrayList(PROP_QUOTA + dot + entry.getKey(), entry.getValue().toString()));
        }

        // load cluster
        if (defaultLoadCluster != null) {
            result.add(Lists.newArrayList(PROP_DEFAULT_LOAD_CLUSTER, defaultLoadCluster));
        } else {
            result.add(Lists.newArrayList(PROP_DEFAULT_LOAD_CLUSTER, ""));
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

        // get resolved ips if user has domain
        Map<String, Set<String>> resolvedIPs = whiteList.getResolvedIPs();
        List<String> ips = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : resolvedIPs.entrySet()) {
            ips.add(entry.getKey() + ":" + Joiner.on(",").join(entry.getValue()));
        }
        if (!ips.isEmpty()) {
            result.add(Lists.newArrayList("resolved IPs", Joiner.on(";").join(ips)));
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
        UserProperty userProperty = new UserProperty();
        userProperty.readFields(in);
        return userProperty;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // user name
        Text.writeString(out, qualifiedUser);

        // user resource
        resource.write(out);

        // load cluster
        if (defaultLoadCluster == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, defaultLoadCluster);
        }
        out.writeInt(clusterToDppConfig.size());
        for (Map.Entry<String, DppConfig> entry : clusterToDppConfig.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }

        // whiteList
        whiteList.write(out);

        // common properties
        commonProperties.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        qualifiedUser = Text.readString(in);

        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_100) {
            long maxConn = in.readLong();
            this.commonProperties.setMaxConn(maxConn);
        }

        // user resource
        resource = UserResource.readIn(in);

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

        // whiteList
        whiteList.readFields(in);

        // common properties
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_100) {
            this.commonProperties = CommonUserProperties.read(in);
        }
    }
}
