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

package org.apache.doris.filesystem.properties;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class HadoopClientOptions {
    private static final Set<String> HDFS_SCHEMES = Set.of("hdfs", "viewfs", "jfs");
    private static final Set<String> REMOTE_SCHEMES = Set.of("hdfs", "viewfs", "jfs", "oss");
    private static final Set<String> USER_CONFIG_KEYS = Set.of(
            "fs.defaultFS",
            "hadoop.username",
            "hadoop.security.authentication",
            "hadoop.security.auth_to_local",
            "hadoop.security.token.service.use_ip",
            "hadoop.rpc.protection",
            "hadoop.kerberos.principal",
            "hadoop.kerberos.keytab",
            "hadoop.kerberos.min.seconds.before.relogin",
            "hadoop.kerberos.keytab.login.autorenewal.enabled",
            "dfs.nameservices",
            "dfs.namenode.rpc-address",
            "dfs.namenode.kerberos.principal",
            "dfs.namenode.kerberos.principal.pattern",
            "dfs.datanode.kerberos.principal",
            "dfs.data.transfer.protection",
            "dfs.encrypt.data.transfer",
            "dfs.encrypt.data.transfer.algorithm",
            "dfs.encrypt.data.transfer.cipher.suites",
            "dfs.encrypt.data.transfer.cipher.key.bitlength",
            "dfs.client.use.datanode.hostname",
            "dfs.client.socket-timeout",
            "dfs.datanode.socket.write.timeout",
            "dfs.domain.socket.path",
            "dfs.client.failover.max.attempts",
            "dfs.client.failover.sleep.base.millis",
            "dfs.client.failover.sleep.max.millis",
            "dfs.client.failover.connection.retries",
            "dfs.client.failover.connection.retries.on.timeouts",
            "dfs.client.failover.random.order",
            "dfs.replication",
            "dfs.blocksize");

    private static final List<String> USER_CONFIG_KEY_PREFIXES = List.of(
            "dfs.ha.namenodes.",
            "dfs.namenode.rpc-address.",
            "dfs.client.read.",
            "dfs.client.hedged.read.",
            "dfs.client.retry.",
            "dfs.client.failover.observer.",
            "dfs.client.block.write.",
            "juicefs.");

    private static final Map<String, String> IMPLEMENTATIONS = Map.of(
            "fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem",
            "fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs",
            "fs.viewfs.impl", "org.apache.hadoop.fs.viewfs.ViewFileSystem",
            "fs.AbstractFileSystem.viewfs.impl", "org.apache.hadoop.fs.viewfs.ViewFs",
            "fs.jfs.impl", "io.juicefs.JuiceFileSystem",
            "fs.AbstractFileSystem.jfs.impl", "io.juicefs.JuiceFS");

    private HadoopClientOptions() {
    }

    public static boolean isHdfsOption(String key, String value) {
        if (key == null || !key.equals(key.trim())) {
            return false;
        }
        String implementation = IMPLEMENTATIONS.get(key);
        if (implementation != null) {
            if (value == null || !implementation.equals(value.trim())) {
                throw new IllegalArgumentException("Unsupported filesystem implementation for property: " + key);
            }
            return true;
        }
        if (key.startsWith("dfs.client.failover.proxy.provider.")) {
            if (value == null || !value.trim().startsWith("org.apache.hadoop.hdfs.server.namenode.ha.")) {
                throw new IllegalArgumentException("Unsupported failover provider for property: " + key);
            }
            return true;
        }
        if (key.startsWith("fs.viewfs.mounttable.")) {
            if (!key.contains(".link")) {
                return key.endsWith(".homedir");
            }
            for (String target : value.split(",", -1)) {
                if (remoteUri(target.trim(), HDFS_SCHEMES) == null) {
                    throw new IllegalArgumentException("Unsupported mount target for property: " + key);
                }
            }
            return true;
        }
        if (USER_CONFIG_KEYS.contains(key) || USER_CONFIG_KEY_PREFIXES.stream().anyMatch(key::startsWith)) {
            return true;
        }
        return HDFS_SCHEMES.stream().anyMatch(scheme -> key.equals("fs." + scheme + ".impl.disable.cache"));
    }

    public static boolean isOssClientOption(String key) {
        return key.startsWith("fs.oss.") && key.equals(key.trim()) && !key.equals("fs.oss.impl");
    }

    public static Map<String, String> forStatement(Map<String, String> properties) {
        Map<String, String> result = new LinkedHashMap<>();
        properties.forEach((name, value) -> {
            String key = name.regionMatches(true, 0, "broker.", 0, 7) ? name.substring(7) : name;
            if (!name.equals(name.trim()) || !key.equals(key.trim())) {
                return;
            }
            String lower = key.toLowerCase(Locale.ROOT);
            if (lower.equals("fs.defaultfs") || lower.equals("oss.hdfs.fs.defaultfs")) {
                URI uri = remoteUri(value, REMOTE_SCHEMES);
                if (uri == null || uri.getRawAuthority() == null) {
                    throw new IllegalArgumentException("Default filesystem must have a remote scheme and authority: "
                            + name);
                }
            } else if (lower.equals("fs.default.name") || lower.equals("_hadoop_config_dir_")
                    || lower.equals("_storage_type_") || lower.endsWith(".config.resources")
                    || lower.startsWith("doris.fs.cache.key.")
                    || (lower.startsWith("fs.") && lower.endsWith(".support"))) {
                throw new IllegalArgumentException("Storage property is not allowed in this statement: " + name);
            } else if (lower.startsWith("fs.") || lower.startsWith("dfs.") || lower.startsWith("hadoop.")
                    || lower.startsWith("juicefs.")) {
                if (!isHdfsOption(key, value) && !isOssClientOption(key)) {
                    return;
                }
            }
            result.put(name, value);
        });
        return result;
    }

    public static boolean isRemoteHadoopUri(String location) {
        return remoteUri(location, REMOTE_SCHEMES) != null;
    }

    private static URI remoteUri(String location, Set<String> schemes) {
        try {
            URI uri = new URI(location);
            return !uri.isOpaque() && uri.getScheme() != null
                    && schemes.contains(uri.getScheme().toLowerCase(Locale.ROOT)) ? uri : null;
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
