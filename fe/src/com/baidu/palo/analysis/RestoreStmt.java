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

package com.baidu.palo.analysis;

import com.baidu.palo.common.util.PrintableMap;

import com.google.common.base.Joiner;

import java.util.List;
import java.util.Map;

public class RestoreStmt extends AbstractBackupStmt {

    public RestoreStmt(LabelName labelName, List<PartitionName> restoreObjNames,
                       String restorePath, Map<String, String> properties) {
        super(labelName, restoreObjNames, restorePath, properties);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RESTORE LABEL ").append(labelName.toSql());
        if (!objNames.isEmpty()) {
            sb.append(" (");
            sb.append(Joiner.on(", ").join(objNames));
            sb.append(")");
        }
        sb.append(" FROM \"").append(remotePath).append("\" PROPERTIES(");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");
        return sb.toString();
    }
}
