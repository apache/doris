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

package com.baidu.palo.common.proc;

import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.common.AnalysisException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/*
 * It describes the information about the authorization(privilege) and the authentication(user)
 * SHOW PROC /access_resource/
 */
public class AccessResourceProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("UserName").add("Password").add("IsAdmin").add("IsSuperuser")
            .add("MaxConn").add("Privilege").build();

    private UserPropertyMgr userPropertyMgr;

    public AccessResourceProcDir(UserPropertyMgr userPropertyMgr) {
        this.userPropertyMgr = userPropertyMgr;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String user) throws AnalysisException {
        if (Strings.isNullOrEmpty(user)) {
            throw new AnalysisException("User[" + user + "] is null");
        }

        return new UserPropertyProcNode(userPropertyMgr, user);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(userPropertyMgr.fetchAccessResourceResult());
        return result;
    }
}
