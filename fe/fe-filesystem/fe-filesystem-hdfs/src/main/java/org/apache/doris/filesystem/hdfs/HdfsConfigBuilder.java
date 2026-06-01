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

/**
 * Builds a Hadoop {@link Configuration} from a {@code Map<String, String>} of properties.
 */
public class HdfsConfigBuilder {

    static final String KEY_PRINCIPAL = "hadoop.kerberos.principal";
    static final String KEY_KEYTAB = "hadoop.kerberos.keytab";

    private HdfsConfigBuilder() {
    }

    /**
     * Builds a Hadoop Configuration from the given properties map.
     * FS caching is disabled for every scheme the provider claims to support so that
     * {@link DFSFileSystem#close()} only closes the FileSystem instances owned by this
     * provider — never a globally-cached instance that another catalog is still using.
     */
    public static Configuration build(Map<String, String> properties) {
        Configuration conf = new HdfsConfiguration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.AbstractFileSystem.hdfs.impl.disable.cache", true);
        for (String scheme : HdfsFileSystemProvider.SUPPORTED_SCHEMES) {
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
