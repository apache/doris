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
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ResourceGroup;
import org.apache.doris.catalog.ResourceType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/*
 * UserProperty contains properties set for a user
 * This user is just qualified by cluster name, not host which it connected from.
 */
public class UserProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(UserProperty.class);

    private static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    private static final String PROP_RESOURCE = "resource";
    private static final String PROP_QUOTA = "quota";
    private static final String PROP_DEFAULT_LOAD_CLUSTER = "default_load_cluster";
    private static final String PROP_LOAD_CLUSTER = "load_cluster";

    // for system user
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

    private String qualifiedUser;

    private long maxConn = 100;
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

    @Deprecated
    private byte[] password;
    @Deprecated
    private boolean isAdmin = false;
    @Deprecated
    private boolean isSuperuser = false;
    @Deprecated
    private Map<String, AccessPrivilege> dbPrivMap = Maps.newHashMap();

    static {
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_MAX_USER_CONNECTIONS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE + ".", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + "."
                + DppConfig.PRIORITY + "$", Pattern.CASE_INSENSITIVE));

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
        return maxConn;
    }

    public WhiteList getWhiteList() {
        return whiteList;
    }

    @Deprecated
    public byte[] getPassword() {
        return password;
    }

    @Deprecated
    public boolean isAdmin() {
        return isAdmin;
    }

    @Deprecated
    public boolean isSuperuser() {
        return isSuperuser;
    }

    @Deprecated
    public Map<String, AccessPrivilege> getDbPrivMap() {
        return dbPrivMap;
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

    public void update(List<Pair<String, String>> properties) throws DdlException {
        // copy
        long newMaxConn = maxConn;
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
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }

        // set
        maxConn = newMaxConn;
        resource = newResource;
        if (newDppConfigs.containsKey(newDefaultLoadCluster)) {
            defaultLoadCluster = newDefaultLoadCluster;
        } else {
            defaultLoadCluster = null;
        }
        clusterToDppConfig = newDppConfigs;
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

        return Pair.create(tmpCluster, dppConfig);
    }

    public List<List<String>> fetchProperty() {
        List<List<String>> result = Lists.newArrayList();
        String dot = SetUserPropertyVar.DOT_SEPARATOR;

        // max user connections
        result.add(Lists.newArrayList(PROP_MAX_USER_CONNECTIONS, String.valueOf(maxConn)));

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
        Text.writeString(out, qualifiedUser);
        out.writeLong(maxConn);

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

        whiteList.write(out);
    }
    public void readFields(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43) {
            // consume the flag of empty user name
            in.readBoolean();
        }
            
        // user name
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
            qualifiedUser = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, Text.readString(in));
        } else {
            qualifiedUser = Text.readString(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43) {
            int passwordLen = in.readInt();
            password = new byte[passwordLen];
            in.readFully(password);

            isAdmin = in.readBoolean();

            if (Catalog.getCurrentCatalogJournalVersion() >= 1) {
                isSuperuser = in.readBoolean();
            }
        }

        maxConn = in.readLong();

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43) {
            int numPriv = in.readInt();
            for (int i = 0; i < numPriv; ++i) {
                String dbName = null;
                if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
                    dbName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, Text.readString(in));
                } else {
                    dbName = Text.readString(in);
                }
                AccessPrivilege ap = AccessPrivilege.valueOf(Text.readString(in));
                dbPrivMap.put(dbName, ap);
            }
        }

        // user resource
        resource = UserResource.readIn(in);

        // load cluster
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_12) {
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
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_21) {
            whiteList.readFields(in);
            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_69) {
                whiteList.convertOldDomainPrivMap(qualifiedUser);
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43) {
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
                if (in.readBoolean()) {
                    // consume cluster name
                    Text.readString(in);
                }
            }
        }
    }
}
