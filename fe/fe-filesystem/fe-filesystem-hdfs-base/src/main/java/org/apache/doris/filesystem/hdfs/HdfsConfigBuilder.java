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

package org.apache.doris.filesystem.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.util.Map;
import java.util.Set;

/**
 * Builds a Hadoop {@link Configuration} from a {@code Map<String, String>} of properties.
 */
public class HdfsConfigBuilder {

    static final String KEY_PRINCIPAL = "hadoop.kerberos.principal";
    static final String KEY_KEYTAB = "hadoop.kerberos.keytab";

    /**
     * Schemes whose Hadoop FS cache must be disabled so {@link DFSFileSystem#close()} only closes
     * instances this module owns. This is the union of every scheme served by a DFSFileSystem in
     * this module, including {@code oss} for the OSS-HDFS (JindoFS) path — note this is broader
     * than {@link HdfsFileSystemProvider#SUPPORTED_SCHEMES}, which is the plain-HDFS routing set
     * and intentionally excludes {@code oss} (owned by {@link OssHdfsFileSystemProvider}).
     */
    static final Set<String> CACHE_DISABLE_SCHEMES = Set.of("hdfs", "viewfs", "ofs", "jfs", "oss");

    private HdfsConfigBuilder() {
    }

    /**
     * Builds a Hadoop Configuration from the given properties map.
     * FS caching is disabled for every scheme this module can serve so that
     * {@link DFSFileSystem#close()} only closes the FileSystem instances owned by this
     * module — never a globally-cached instance that another catalog is still using.
     */
    public static Configuration build(Map<String, String> properties) {
        Configuration conf = new HdfsConfiguration();
        // Pin the conf classloader to this plugin's loader, mirroring HmsConfHelper.createHiveConf and the
        // connector conf builders (Paimon/Iceberg/Hive/Hudi). new HdfsConfiguration() captures the LIVE
        // thread-context classloader into the conf's OWN classLoader field. DFSFileSystem is built lazily on
        // first HDFS access, which runs under a connector's plugin loader (PluginDrivenScanNode.onPluginClassLoader
        // pins the TCCL there for the scan), so the captured CL would be that connector loader. Hadoop then
        // resolves impl classes via Configuration.getClass using this field, NOT the live TCCL: e.g.
        // RPC.getProtocolEngine loads ProtobufRpcEngine2 through it. Left unpinned that yields the connector's
        // hadoop-common copy of ProtobufRpcEngine2 while RPC/RpcEngine come from the engine copy, giving
        // "class ProtobufRpcEngine2 cannot be cast to class RpcEngine". DFSFileSystem.getHadoopFs pins the live
        // TCCL for FileSystem.get (ServiceLoader discovery) but cannot fix this conf-cached CL.
        conf.setClassLoader(HdfsConfigBuilder.class.getClassLoader());
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.AbstractFileSystem.hdfs.impl.disable.cache", true);
        for (String scheme : CACHE_DISABLE_SCHEMES) {
            conf.setBoolean("fs." + scheme + ".impl.disable.cache", true);
            conf.setBoolean("fs.AbstractFileSystem." + scheme + ".impl.disable.cache", true);
        }
        properties.forEach((k, v) -> {
            if (v != null && !v.isEmpty()) {
                conf.set(k, v);
            }
        });
        return conf;
    }

    /**
     * Returns true if both Kerberos principal and keytab are present in the properties.
     */
    public static boolean isKerberosEnabled(Map<String, String> properties) {
        return properties.containsKey(KEY_PRINCIPAL) && properties.containsKey(KEY_KEYTAB);
    }
}
