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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Handler.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.nereids.PLParser.Declare_handler_itemContext;
import org.apache.doris.plsql.Signal.Type;

/**
 * PL/SQL condition and exception handler
 */
public class Handler {
    public enum ExecType {
        CONTINUE, EXIT
    }

    ExecType execType;
    Type type;
    String value;
    Scope scope;
    Declare_handler_itemContext ctx;

    Handler(ExecType execType, Type type, String value, Scope scope,
            Declare_handler_itemContext ctx) {
        this.execType = execType;
        this.type = type;
        this.value = value;
        this.scope = scope;
        this.ctx = ctx;
    }
}
