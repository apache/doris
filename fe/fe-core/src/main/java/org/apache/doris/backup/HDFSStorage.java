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

package org.apache.doris.backup;

import org.apache.doris.common.UserException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// TODO: extend BlobStorage
public class HDFSStorage {
    public static final String HDFS_DEFAULT_FS = "fs.defaultFS";
    public static final String USER = "hdfs_user";
    public static final String NAME_SERVICES = "dfs.nameservices";
    public static final String NAME_NODES = "dfs.ha.namenodes";
    public static final String RPC_ADDRESS = "dfs.namenode.rpc-address";
    public static final String FAILOVER_PROXY = "dfs.client.failover.proxy.provider";
    public static final String AUTHENTICATION = "hadoop.security.authentication";
    public static final String KERBEROS_PRINCIPAL = "kerberos_principal";
    public static final String KERB_TICKET_CACHE_PATH = "kerb_ticket_cache_path";
    public static final String TOKEN = "token";

    public static Set<String> keySets = new HashSet<>(Arrays.asList(HDFS_DEFAULT_FS, USER,
            NAME_SERVICES, NAME_NODES, RPC_ADDRESS, FAILOVER_PROXY,
            AUTHENTICATION,
            KERBEROS_PRINCIPAL, KERB_TICKET_CACHE_PATH,
            TOKEN));


    public static void checkHDFS(Map<String, String> properties) throws UserException {
        if (!properties.containsKey(HDFS_DEFAULT_FS)) {
            throw new UserException(HDFS_DEFAULT_FS + " not found. This is required field");
        }
        for (String key : properties.keySet()) {
            if (!keySets.contains(key)) {
                throw new UserException("Unknown properties " + key);
            }
        }
    }
}
