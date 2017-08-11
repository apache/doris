// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.google.common.collect.ImmutableSortedMap;

import java.util.List;

// 用于表示用户对于某种资源的访问权限
// 优于当前支持两种权限，所以当前使用简单的优先级标志比较完成
public enum AccessPrivilege {
    READ_ONLY(1, "READ_ONLY"),
    READ_WRITE(2, "READ_WRITE"),
    ALL(3, "ALL");

    private int flag;
    private String desc;

    private static final ImmutableSortedMap<String, AccessPrivilege> NAME_MAP =
            ImmutableSortedMap.<String, AccessPrivilege>orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .put("READ_ONLY", READ_ONLY)
                    .put("READ_WRITE", READ_WRITE)
                    .put("ALL", ALL)
                    .build();

    private AccessPrivilege(int flag, String desc) {
        this.flag = flag;
        this.desc = desc;
    }

    public static boolean contains(AccessPrivilege p1, AccessPrivilege p2) {
        return p1.flag >= p2.flag;
    }

    public boolean contains(AccessPrivilege priv) {
        return contains(this, priv);
    }

    public static AccessPrivilege fromName(String privStr) {
        return NAME_MAP.get(privStr);
    }

    public static AccessPrivilege merge(List<AccessPrivilege> privileges) {
        if (privileges == null || privileges.isEmpty()) {
            return null;
        }

        AccessPrivilege privilege = null;
        for (AccessPrivilege iter : privileges) {
            if (privilege == null) {
                privilege = iter;
            } else {
                if (iter.flag > privilege.flag) {
                    privilege = iter;
                }
            }
        }

        return privilege;
    }

    @Override
    public String toString() {
        return desc;
    }
}
