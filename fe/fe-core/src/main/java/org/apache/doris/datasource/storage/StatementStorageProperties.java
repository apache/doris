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

package org.apache.doris.datasource.storage;

import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Hadoop options that can come from SQL, before operator resources or plugin defaults are added. */
public final class StatementStorageProperties {
    private static final Set<String> REMOTE_SCHEMES = Set.of("hdfs", "viewfs", "jfs", "oss");

    private static final Pattern VIEWFS_LINK = Pattern.compile("fs\\.viewfs\\.mounttable\\.[^.]+\\.link\\./.*");
    private static final Pattern VIEWFS_HOME = Pattern.compile("fs\\.viewfs\\.mounttable\\.[^.]+\\.homedir");

    private StatementStorageProperties() {
    }

    public static void check(Map<String, String> properties) {
        String adminProperty = null;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String name = entry.getKey();
            String key = name.trim().toLowerCase(Locale.ROOT);
            if (key.startsWith("broker.")) {
                key = key.substring("broker.".length()).trim();
            }
            String value = entry.getValue();
            if (key.equals("fs.defaultfs") || key.equals("oss.hdfs.fs.defaultfs")) {
                if (!hasRemoteAuthority(value)) {
                    throw new AnalysisException("Default filesystem must have a remote scheme and authority: " + name);
                }
            } else if (isReserved(key, value)) {
                throw new AnalysisException("Storage property is not allowed in this statement: " + name);
            } else if (adminProperty == null && requiresAdmin(key)) {
                adminProperty = name;
            }
        }
        if (adminProperty != null) {
            ConnectContext ctx = ConnectContext.get();
            checkAdmin(ctx, adminProperty);
            StatementContext statement = ctx.getStatementContext();
            if (statement != null) {
                statement.setAdminStorageProperty(adminProperty);
            }
        }
    }

    public static void checkAdmin(ConnectContext ctx, String property) {
        if (ctx == null || ctx.getCurrentUserIdentity() == null
                || !ctx.getEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Storage property requires ADMIN_PRIV: " + property);
        }
    }

    private static boolean requiresAdmin(String key) {
        return !key.equals("hadoop.username") && (key.startsWith("hadoop.") || key.startsWith("dfs.")
                || key.startsWith("fs.") || key.startsWith("juicefs.") || key.startsWith("hdfs.authentication.")
                || key.equals("hdfs.impersonation.enabled")
                || key.equals("ipc.client.fallback-to-simple-auth-allowed"));
    }

    private static boolean isReserved(String key, String value) {
        if (key.startsWith("fs.viewfs.")) {
            if (VIEWFS_LINK.matcher(key).matches()) {
                return !hasRemoteAuthority(value);
            }
            return !VIEWFS_HOME.matcher(key).matches() || !value.startsWith("/");
        }
        return key.equals("fs.default.name") || key.equals("_hadoop_config_dir_") || key.equals("_storage_type_")
                || (key.startsWith("fs.") && (key.endsWith(".impl") || key.endsWith(".support")))
                || key.endsWith(".config.resources") || key.startsWith("doris.fs.cache.key.");
    }

    private static boolean hasRemoteAuthority(String value) {
        URI uri = remoteHadoopUri(value);
        return uri != null && uri.getRawAuthority() != null;
    }

    public static boolean isRemoteHadoopUri(String location) {
        return remoteHadoopUri(location) != null;
    }

    private static URI remoteHadoopUri(String location) {
        try {
            URI uri = new URI(location);
            return !uri.isOpaque() && uri.getScheme() != null
                    && REMOTE_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT)) ? uri : null;
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
