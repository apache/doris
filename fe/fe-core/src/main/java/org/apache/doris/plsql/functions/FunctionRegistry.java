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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/FunctionRegistry.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.nereids.PLParser.Create_function_stmtContext;
import org.apache.doris.nereids.PLParser.Create_procedure_stmtContext;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;

import java.util.List;

public interface FunctionRegistry {
    boolean exec(FuncNameInfo procedureName, Expr_func_paramsContext ctx);

    void addUserFunction(Create_function_stmtContext ctx);

    void addUserProcedure(Create_procedure_stmtContext ctx);

    void save(FuncNameInfo procedureName, String source, boolean isForce);

    boolean exists(FuncNameInfo procedureName);

    void remove(FuncNameInfo procedureName);

    void removeCached(String name);

    void showProcedure(List<List<String>> columns, String dbFilter, String procFilter);

    void showCreateProcedure(FuncNameInfo procedureName, List<List<String>> columns);
}
