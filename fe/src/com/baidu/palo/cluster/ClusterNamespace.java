// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.cluster;

import com.google.common.base.Strings;

import com.baidu.palo.catalog.UserPropertyMgr;

public class ClusterNamespace {

    public static final String CLUSTER_DELIMITER = ":";

    /**
     * db full name : cluster-db
     * 
     * @param cluster
     * @param db
     * @return
     */
    public static String getDbFullName(String cluster, String db) {
        return linkString(cluster, db);
    }

    /**
     * user full name : cluster-usr
     * 
     * @param cluster
     * @param usr
     * @return
     */
    public static String getUserFullName(String cluster, String usr) {
        return linkString(cluster, usr);
    }

    private static boolean checkName(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return false;
        }
        final String[] ele = str.split(CLUSTER_DELIMITER);
        return (ele.length > 1) ? false : true;
    }

    private static String linkString(String str1, String str2) {
        if (Strings.isNullOrEmpty(str1) || Strings.isNullOrEmpty(str2)) {
            return null;
        }
        if (str2.contains(CLUSTER_DELIMITER) || str2.equals(UserPropertyMgr.getRootName())) {
            return str2;
        }
        final StringBuilder sb = new StringBuilder(str1);
        sb.append(CLUSTER_DELIMITER).append(str2);
        return sb.toString();
    }

    /**
     * get db name from cluster-db
     * 
     * @param db
     * @return
     */
    public static String getDbNameFromFullName(String db) {
        if (checkName(db)) {
            return null;
        }
        return extract(db, 1);
    }

    /**
     * get user name from cluster-user
     * 
     * @param usr
     * @return
     */
    public static String getUsrNameFromFullName(String usr) {
        if (checkName(usr)) {
            return null;
        }
        return extract(usr, 1);
    }

    /**
     * get cluster name from cluster-user
     * 
     * @param str
     * @return
     */
    public static String getClusterNameFromFullName(String str) {
        if (checkName(str)) {
            return null;
        }
        return extract(str, 0);
    }

    private static String extract(String db, int index) {
        final String[] ele = db.split(CLUSTER_DELIMITER);
        return ele[index];
    }
}
