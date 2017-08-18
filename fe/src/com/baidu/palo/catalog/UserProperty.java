// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.SetUserPropertyVar;
import com.baidu.palo.analysis.SetVar;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.LoadException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.load.DppConfig;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.system.SystemInfoService;

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

public class UserProperty implements Writable {
    private static final String MAX_USER_CONNECTIONS = "max_user_connections";
    private static final String RESOURCE = "resource";
    private static final String QUOTA = "quota";
    private static final String DEFAULT_LOAD_CLUSTER = "default_load_cluster";
    private static final String LOAD_CLUSTER = "load_cluster";

    // for superuser or root
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

     // 用户所属的cluster
    String clusterName;
    // 此处保留UserName是为了便于序列化信息
    String userName;
    // 用户的密码hash值，当前存储的是SHA1(SHA1('password'))
    // 如果用户没有密码，这里存储的应该是byte[0]
    private byte[] password;
    // 用户所拥有的数据库权限
    private Map<String, AccessPrivilege> dbPrivMap;
    // 用户是否是管理员
    private boolean isAdmin;
    private boolean isSuperuser = false;
    // 用户最大连接数
    private long maxConn;
    // Resource belong to this user.
    private UserResource resource;
    // load cluster
    private String defaultLoadCluster;
    private Map<String, DppConfig> clusterToDppConfig;

    // whilelist
    WhiteList whiteList;

    static {
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + MAX_USER_CONNECTIONS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + RESOURCE + ".", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile(
                "^" + LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + "." + DppConfig.PRIORITY + "$",
                Pattern.CASE_INSENSITIVE));

        COMMON_PROPERTIES.add(Pattern.compile("^" + QUOTA + ".", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + DEFAULT_LOAD_CLUSTER + "$", Pattern.CASE_INSENSITIVE));
        COMMON_PROPERTIES.add(Pattern.compile("^" + LOAD_CLUSTER + "." + DppConfig.CLUSTER_NAME_REGEX + ".",
                Pattern.CASE_INSENSITIVE));
    }

    public UserProperty() {
        clusterName = "";
        userName = null;
        password = new byte[0];
        dbPrivMap = Maps.newHashMap();
        isAdmin = false;
        maxConn = Config.max_conn_per_user;
        resource = new UserResource(1000);
        defaultLoadCluster = null;
        clusterToDppConfig = Maps.newHashMap();
        whiteList = new WhiteList();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String name) {    
        this.clusterName = name;
    }

    public void setIsSuperuser(boolean isSuperuser) {
        this.isSuperuser = isSuperuser;
    }

    public String getUser() {
        return userName;
    }

    public void setUser(String userName) {
        this.userName = userName;
        whiteList.setUser(userName);
    }

    public void setIsAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
    }

    public boolean isAdmin() {
        return isAdmin;
    }

    public boolean isSuperuser() {
        if (isAdmin) {
            return true;
        }
        return isSuperuser;
    }

    public long getMaxConn() {
        return maxConn;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public void update(List<SetVar> propertyVarList) throws DdlException {
        // copy
        long newMaxConn = maxConn;
        UserResource newResource = resource.getCopiedUserResource();
        String newDefaultLoadCluster = defaultLoadCluster;
        Map<String, DppConfig> newDppConfigs = Maps.newHashMap(clusterToDppConfig);

        // update
        for (SetVar var : propertyVarList) {
            SetUserPropertyVar propertyVar = (SetUserPropertyVar) var;
            String key = propertyVar.getPropertyKey();
            String value = propertyVar.getPropertyValue();

            String[] keyArr = key.split("\\" + SetUserPropertyVar.DOT_SEPARATOR);
            if (keyArr[0].equalsIgnoreCase(MAX_USER_CONNECTIONS)) {
                // set property "max_user_connections" = "1000"
                if (keyArr.length != 1) {
                    throw new DdlException(MAX_USER_CONNECTIONS + " format error");
                }

                try {
                    newMaxConn = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(MAX_USER_CONNECTIONS + " is not number");
                }

                if (newMaxConn <= 0 || newMaxConn > 10000) {
                    throw new DdlException(MAX_USER_CONNECTIONS + " is not valid, must between 1 and 10000");
                }
            } else if (keyArr[0].equalsIgnoreCase(RESOURCE)) {
                // set property "resource.cpu_share" = "100"
                if (keyArr.length != 2) {
                    throw new DdlException(RESOURCE + " format error");
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
            } else if (keyArr[0].equalsIgnoreCase(QUOTA)) {
                // set property "quota.normal" = "100"
                if (keyArr.length != 2) {
                    throw new DdlException(QUOTA + " format error");
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
            } else if (keyArr[0].equalsIgnoreCase(LOAD_CLUSTER)) {
                updateLoadCluster(keyArr, value, newDppConfigs);
            } else if (keyArr[0].equalsIgnoreCase(DEFAULT_LOAD_CLUSTER)) {
                // set property "default_load_cluster" = "cluster1"
                if (keyArr.length != 1) {
                    throw new DdlException(DEFAULT_LOAD_CLUSTER + " format error");
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
        if (keyArr.length == 1 && value == null) {
            // set property "load_cluster" = null
            newDppConfigs.clear();
        } else if (keyArr.length == 2 && value == null) {
            // set property "load_cluster.cluster1" = null
            String cluster = keyArr[1];
            newDppConfigs.remove(cluster);
        } else if (keyArr.length == 3 && value == null) {
            // set property "load_cluster.cluster1.xxx" = null
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
            throw new DdlException(LOAD_CLUSTER + " format error");
        }
    }

    // 用于判断一个用户是否对于需要访问的数据库有相应的权限
    public boolean checkAccess(String db, AccessPrivilege priv) {
        if (isSuperuser()) {
            return true;
        }
        // information_schema is case insensitive
        if (db.equalsIgnoreCase(InfoSchemaDb.getDatabaseName())) {
            db = InfoSchemaDb.getDatabaseName();
        }
        AccessPrivilege dbPriv = dbPrivMap.get(db);
        if (dbPriv == null) {
            return false;
        }
        return dbPriv.contains(priv);
    }

    // 修改用户已有的权限, 有可能当前对此DB无权限, 有可能已经有权限了，只是修改
    // 无论如何，直接覆盖就OK了。
    public void setAccess(String db, AccessPrivilege priv) {
        dbPrivMap.put(db, priv);
    }

    public void revokeAccess(String db) throws DdlException {
        if (!dbPrivMap.containsKey(db)) {
            throw new DdlException("User[" + userName + "] has no privilege on database[" + db + "]");
        }

        // just remove all privilege
        dbPrivMap.remove(db);
    }

    public UserResource getResource() {
        return resource;
    }

    public String getDefaultLoadCluster() {
        return defaultLoadCluster;
    }

    public Pair<String, DppConfig> getClusterInfo(String cluster) {
        if (cluster == null) {
            cluster = defaultLoadCluster;
        }

        DppConfig dppConfig = null;
        if (cluster != null) {
            dppConfig = clusterToDppConfig.get(cluster);
            if (dppConfig != null) {
                dppConfig = dppConfig.getCopiedDppConfig();
            }
        }

        return Pair.create(cluster, dppConfig);
    }

    public List<List<String>> fetchProperty() {
        List<List<String>> result = Lists.newArrayList();
        String dot = SetUserPropertyVar.DOT_SEPARATOR;

        // max user connections
        result.add(Lists.newArrayList(MAX_USER_CONNECTIONS, String.valueOf(maxConn)));

        // resource
        ResourceGroup group = resource.getResource();
        for (Map.Entry<ResourceType, Integer> entry : group.getQuotaMap().entrySet()) {
            result.add(Lists.newArrayList(RESOURCE + dot + entry.getKey().getDesc().toLowerCase(),
                    entry.getValue().toString()));
        }

        // quota
        Map<String, AtomicInteger> groups = resource.getShareByGroup();
        for (Map.Entry<String, AtomicInteger> entry : groups.entrySet()) {
            result.add(Lists.newArrayList(QUOTA + dot + entry.getKey(), entry.getValue().toString()));
        }

        // load cluster
        if (defaultLoadCluster != null) {
            result.add(Lists.newArrayList(DEFAULT_LOAD_CLUSTER, defaultLoadCluster));
        } else {
            result.add(Lists.newArrayList(DEFAULT_LOAD_CLUSTER, ""));
        }

        for (Map.Entry<String, DppConfig> entry : clusterToDppConfig.entrySet()) {
            String cluster = entry.getKey();
            DppConfig dppConfig = entry.getValue();
            String clusterPrefix = LOAD_CLUSTER + dot + cluster + dot;

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

    public String fetchPrivilegeResult() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (Map.Entry<String, AccessPrivilege> entry : dbPrivMap.entrySet()) {
            if (!isFirst) {
                stringBuilder.append(", ");
            }
            String dbName = entry.getKey();
            AccessPrivilege privilege = entry.getValue();
            stringBuilder.append(dbName).append("(").append(privilege.name()).append(")");
            isFirst = false;
        }
        return stringBuilder.toString();
    }

    public void write(DataOutput out) throws IOException {
        if (userName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, userName);
        }
        out.writeInt(password.length);
        out.write(password);
        out.writeBoolean(isAdmin);
        out.writeBoolean(isSuperuser);
        out.writeLong(maxConn);

        int numPriv = dbPrivMap.size();
        out.writeInt(numPriv);
        for (Map.Entry<String, AccessPrivilege> entry : dbPrivMap.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().name());
        }
        // User resource
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
        if (userName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, clusterName);
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
                userName = ClusterNamespace.getUserFullName(SystemInfoService.DEFAULT_CLUSTER, Text.readString(in));
            } else {
                userName = Text.readString(in);
            }
        }
        int passwordLen = in.readInt();
        password = new byte[passwordLen];
        in.readFully(password);
        isAdmin = in.readBoolean();

        if (Catalog.getCurrentCatalogJournalVersion() >= 1) {
            isSuperuser = in.readBoolean();
        }

        maxConn = in.readLong();

        int numPriv = in.readInt();
        for (int i = 0; i < numPriv; ++i) {
            String dbName;
            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
                dbName = ClusterNamespace.getDbFullName(SystemInfoService.DEFAULT_CLUSTER, Text.readString(in));
            } else {
                dbName = Text.readString(in);
            }
            AccessPrivilege accessPrivilege = AccessPrivilege.valueOf(Text.readString(in));
            dbPrivMap.put(dbName, accessPrivilege);
        }
        // User resource
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
            whiteList.setUser(userName);
            whiteList.readFields(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            if (in.readBoolean()) {
                clusterName = Text.readString(in);
            }
        }
    }

    public static UserProperty read(DataInput in) throws IOException {
        UserProperty userProperty = new UserProperty();
        userProperty.readFields(in);
        return userProperty;
    }

    public WhiteList getWhiteList() {
        return whiteList;
    }
}
