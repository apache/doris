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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.SetUserPropertyVar;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
public class UserProperty {
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

    public static final String PROP_QUOTA = "quota";

    public static final String PROP_DEFAULT_INIT_CATALOG = "default_init_catalog";
    public static final String PROP_WORKLOAD_GROUP = "default_workload_group";

    public static final String DEFAULT_CLOUD_CLUSTER = "default_cloud_cluster";
    public static final String DEFAULT_COMPUTE_GROUP = "default_compute_group";

    // for system user
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

    @SerializedName(value = "qu", alternate = {"qualifiedUser"})
    private String qualifiedUser;

    @SerializedName(value = "cp", alternate = {"commonProperties"})
    private CommonUserProperties commonProperties = new CommonUserProperties();

    @SerializedName(value = "dcc", alternate = {"defaultCloudCluster"})
    private String defaultCloudCluster = null;

    public static final Set<Tag> INVALID_RESOURCE_TAGS;

    static {
        INVALID_RESOURCE_TAGS = Sets.newHashSet();
        INVALID_RESOURCE_TAGS.add(Tag.INVALID_TAG);
    }

    static {
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_MAX_USER_CONNECTIONS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE + ".", Pattern.CASE_INSENSITIVE));
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
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_DEFAULT_INIT_CATALOG + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_WORKLOAD_GROUP + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + DEFAULT_CLOUD_CLUSTER + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + DEFAULT_COMPUTE_GROUP + "$", Pattern.CASE_INSENSITIVE));
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

    public String getInitCatalog() {
        return commonProperties.getInitCatalog();
    }

    public String getWorkloadGroup() {
        return commonProperties.getWorkloadGroup();
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
        String initCatalog = this.commonProperties.getInitCatalog();
        String workloadGroup = this.commonProperties.getWorkloadGroup();

        String newDefaultCloudCluster = defaultCloudCluster;

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
            } else if (keyArr[0].equalsIgnoreCase(DEFAULT_CLOUD_CLUSTER)) {
                newDefaultCloudCluster = checkCloudDefaultCluster(keyArr, value, DEFAULT_CLOUD_CLUSTER, isReplay);
            } else if (keyArr[0].equalsIgnoreCase(DEFAULT_COMPUTE_GROUP)) {
                newDefaultCloudCluster = checkCloudDefaultCluster(keyArr, value, DEFAULT_COMPUTE_GROUP, isReplay);
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
            } else if (keyArr[0].equalsIgnoreCase(PROP_DEFAULT_INIT_CATALOG)) {
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_DEFAULT_INIT_CATALOG + " format error");
                }
                CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(value);
                if (catalog == null) {
                    throw new DdlException("catalog " + value + " not exists");
                }
                initCatalog = value;
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
        this.commonProperties.setInitCatalog(initCatalog);
        this.commonProperties.setWorkloadGroup(workloadGroup);
        defaultCloudCluster = newDefaultCloudCluster;
    }

    private String checkCloudDefaultCluster(String[] keyArr, String value, String defaultComputeGroup, boolean isReplay)
            throws ComputeGroupException, DdlException {
        // isReplay not check auth, not throw exception
        if (isReplay) {
            return value;
        }
        // check cluster auth
        if (!Strings.isNullOrEmpty(value) && !Env.getCurrentEnv().getAccessManager().checkCloudPriv(
            new UserIdentity(qualifiedUser, "%"), value, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new ComputeGroupException(String.format("set default compute group failed, "
                + "user %s has no permission to use compute group '%s', please grant use privilege first ",
                qualifiedUser, value),
                ComputeGroupException.FailedTypeEnum.CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP);
        }
        // set property "DEFAULT_CLOUD_CLUSTER" = "cluster1"
        if (keyArr.length != 1) {
            throw new DdlException(defaultComputeGroup + " format error");
        }
        if (value == null) {
            value = "";
        }
        return value;
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

    public String getDefaultCloudCluster() {
        return defaultCloudCluster;
    }

    public List<List<String>> fetchProperty() {
        List<List<String>> result = Lists.newArrayList();

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

        // init catalog
        result.add(Lists.newArrayList(PROP_DEFAULT_INIT_CATALOG, String.valueOf(commonProperties.getInitCatalog())));

        result.add(Lists.newArrayList(PROP_WORKLOAD_GROUP, String.valueOf(commonProperties.getWorkloadGroup())));

        // default cloud cluster
        if (defaultCloudCluster != null) {
            result.add(Lists.newArrayList(DEFAULT_CLOUD_CLUSTER, defaultCloudCluster));
        } else {
            result.add(Lists.newArrayList(DEFAULT_CLOUD_CLUSTER, ""));
        }

        // default compute group
        if (defaultCloudCluster != null) {
            result.add(Lists.newArrayList(DEFAULT_COMPUTE_GROUP, defaultCloudCluster));
        } else {
            result.add(Lists.newArrayList(DEFAULT_COMPUTE_GROUP, ""));
        }

        // sort
        Collections.sort(result, Comparator.comparing(o -> o.get(0)));

        return result;
    }
}
