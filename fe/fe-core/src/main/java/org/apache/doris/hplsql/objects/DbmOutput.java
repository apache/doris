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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/objects/DbmOutput.java
// and modified by Doris

package org.apache.doris.hplsql.objects;

import org.apache.doris.hplsql.Console;
import org.apache.doris.hplsql.Var;

import java.util.List;

public class DbmOutput implements org.apache.doris.hplsql.objects.HplObject {
    private final org.apache.doris.hplsql.objects.HplClass hplClass;
    private Console console;

    public DbmOutput(org.apache.doris.hplsql.objects.HplClass hplClass) {
        this.hplClass = hplClass;
    }

    public void initialize(Console console) {
        this.console = console;
    }

    @Override
    public org.apache.doris.hplsql.objects.HplClass hplClass() {
        return hplClass;
    }

    public Var putLine(List<Var> params) {
        if (!params.isEmpty()) {
            console.printLine(params.get(0).toString());
        }
        return null;
    }
}
