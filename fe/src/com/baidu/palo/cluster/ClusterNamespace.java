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

package com.baidu.palo.cluster;

import com.google.common.base.Strings;

import com.baidu.palo.catalog.UserPropertyMgr;

/**
 * used to isolate the use for the database name and user name in the catalog, 
 * all using the database name and user name place need to call the appropriate 
 * method to makeup full name or get real name, full name is made up generally 
 * in stmt's analyze.
 * 
 * @author chenhao
 */

public class ClusterNamespace {

    public static final String CLUSTER_DELIMITER = ":";

    public static String getFullName(String cluster, String name) {
        return linkString(cluster, name);
    }

    public static String getClusterNameFromFullName(String fullName) {
        if (!checkName(fullName)) {
            return null;
        }
        return extract(fullName, 0);
    }

    public static String getNameFromFullName(String fullName) {
        if (!checkName(fullName)) {
            return fullName;
        }
        return extract(fullName, 1);
    }

    private static boolean checkName(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return false;
        }
        final String[] ele = str.split(CLUSTER_DELIMITER);
        return (ele.length > 1) ? true : false;
    }

    private static String linkString(String cluster, String name) {
        if (Strings.isNullOrEmpty(cluster) || Strings.isNullOrEmpty(name)) {
            return null;
        }
        if (name.contains(CLUSTER_DELIMITER) || name.equals(UserPropertyMgr.getRootName())) {
            return name;
        }
        final StringBuilder sb = new StringBuilder(cluster);
        sb.append(CLUSTER_DELIMITER).append(name);
        return sb.toString();
    }

    private static String extract(String fullName, int index) {
        final String[] ele = fullName.split(CLUSTER_DELIMITER);
        return ele[index];
    }
}
