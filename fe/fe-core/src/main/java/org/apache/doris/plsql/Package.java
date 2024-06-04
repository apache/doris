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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Package.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.nereids.PLParser.Create_function_stmtContext;
import org.apache.doris.nereids.PLParser.Create_package_body_stmtContext;
import org.apache.doris.nereids.PLParser.Create_package_stmtContext;
import org.apache.doris.nereids.PLParser.Create_procedure_stmtContext;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Package_body_itemContext;
import org.apache.doris.nereids.PLParser.Package_spec_itemContext;
import org.apache.doris.plsql.functions.BuiltinFunctions;
import org.apache.doris.plsql.functions.InMemoryFunctionRegistry;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Program package
 */
public class Package {

    private String name;
    private List<Var> vars = new ArrayList<>();
    private List<String> publicFuncs = new ArrayList<>();
    private List<String> publicProcs = new ArrayList<>();

    HashMap<String, Create_function_stmtContext> func = new HashMap<>();
    HashMap<String, Create_procedure_stmtContext> proc = new HashMap<>();

    boolean allMembersPublic = false;

    Exec exec;
    InMemoryFunctionRegistry function;
    boolean trace = false;

    Package(String name, Exec exec, BuiltinFunctions builtinFunctions) {
        this.name = name;
        this.exec = exec;
        this.function = new InMemoryFunctionRegistry(exec, builtinFunctions);
        this.trace = exec.getTrace();
    }

    /**
     * Add a local variable
     */
    public void addVariable(Var var) {
        vars.add(var);
    }

    /**
     * Find the variable by name
     */
    public Var findVariable(String name) {
        for (Var var : vars) {
            if (name.equalsIgnoreCase(var.getName())) {
                return var;
            }
        }
        return null;
    }

    /**
     * Create the package specification
     */
    public void createSpecification(Create_package_stmtContext ctx) {
        int cnt = ctx.package_spec().package_spec_item().size();
        for (int i = 0; i < cnt; i++) {
            Package_spec_itemContext c = ctx.package_spec().package_spec_item(i);
            if (c.declare_stmt_item() != null) {
                visit(c);
            } else if (c.FUNCTION() != null) {
                publicFuncs.add(c.ident_pl().getText().toUpperCase());
            } else if (c.PROC() != null || c.PROCEDURE() != null) {
                publicProcs.add(c.ident_pl().getText().toUpperCase());
            }
        }
    }

    /**
     * Create the package body
     */
    public void createBody(Create_package_body_stmtContext ctx) {
        int cnt = ctx.package_body().package_body_item().size();
        for (int i = 0; i < cnt; i++) {
            Package_body_itemContext c = ctx.package_body().package_body_item(i);
            if (c.declare_stmt_item() != null) {
                visit(c);
            } else if (c.create_function_stmt() != null) {
                func.put(c.create_function_stmt().multipartIdentifier().getText().toUpperCase(),
                        c.create_function_stmt());
            } else if (c.create_procedure_stmt() != null) {
                proc.put(c.create_procedure_stmt().multipartIdentifier().getText().toUpperCase(),
                        c.create_procedure_stmt());
            }
        }
    }

    /**
     * Execute function
     */
    public boolean execFunc(String name, Expr_func_paramsContext ctx) {
        Create_function_stmtContext f = func.get(name.toUpperCase());
        if (f == null) {
            return execProc(name, ctx, false /*trace error if not exists*/);
        }
        if (trace) {
            trace(ctx, "EXEC PACKAGE FUNCTION " + this.name + "." + name);
        }
        ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
        exec.enterScope(Scope.Type.ROUTINE, this);
        InMemoryFunctionRegistry.setCallParameters(name, ctx, actualParams, f.create_routine_params(), null, exec);
        visit(f.single_block_stmt());
        exec.leaveScope();
        return true;
    }

    /**
     * Execute procedure
     */
    public boolean execProc(String name, Expr_func_paramsContext ctx,
            boolean traceNotExists) {
        Create_procedure_stmtContext p = proc.get(name.toUpperCase());
        if (p == null) {
            if (trace && traceNotExists) {
                trace(ctx, "Package procedure not found: " + this.name + "." + name);
            }
            return false;
        }
        if (trace) {
            trace(ctx, "EXEC PACKAGE PROCEDURE " + this.name + "." + name);
        }
        ArrayList<Var> actualParams = function.getActualCallParameters(ctx);
        HashMap<String, Var> out = new HashMap<String, Var>();
        exec.enterScope(Scope.Type.ROUTINE, this);
        exec.callStackPush(name);
        if (p.declare_block_inplace() != null) {
            visit(p.declare_block_inplace());
        }
        if (p.create_routine_params() != null) {
            InMemoryFunctionRegistry.setCallParameters(name, ctx, actualParams, p.create_routine_params(), out, exec);
        }
        visit(p.procedure_block());
        exec.callStackPop();
        exec.leaveScope();
        for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters
            exec.setVariable(i.getKey(), i.getValue());
        }
        return true;
    }

    /**
     * Set whether all members are public (when package specification is missed) or not
     */
    void setAllMembersPublic(boolean value) {
        allMembersPublic = value;
    }

    /**
     * Execute rules
     */
    Integer visit(ParserRuleContext ctx) {
        return exec.visit(ctx);
    }

    /**
     * Trace information
     */
    public void trace(ParserRuleContext ctx, String message) {
        if (trace) {
            exec.trace(ctx, message);
        }
    }
}
