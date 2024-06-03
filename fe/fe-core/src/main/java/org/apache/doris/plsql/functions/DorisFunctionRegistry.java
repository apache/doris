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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/HmsFunctionRegistry.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.PLLexer;
import org.apache.doris.nereids.PLParser;
import org.apache.doris.nereids.PLParser.Create_function_stmtContext;
import org.apache.doris.nereids.PLParser.Create_procedure_stmtContext;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParserBaseVisitor;
import org.apache.doris.nereids.parser.CaseInsensitiveStream;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.Scope;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.metastore.PlsqlMetaClient;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.qe.ConnectContext;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DorisFunctionRegistry implements FunctionRegistry {
    private final Exec exec;
    private final boolean trace;
    private final PlsqlMetaClient client;
    private final BuiltinFunctions builtinFunctions;
    private final Map<String, ParserRuleContext> cache = new HashMap<>();

    public DorisFunctionRegistry(Exec e, PlsqlMetaClient client, BuiltinFunctions builtinFunctions) {
        this.exec = e;
        this.client = client;
        this.builtinFunctions = builtinFunctions;
        this.trace = exec.getTrace();
    }

    @Override
    public boolean exists(FuncNameInfo procedureName) {
        return isCached(procedureName.toString()) || getProc(procedureName).isPresent();
    }

    @Override
    public void remove(FuncNameInfo procedureName) {
        try {
            client.dropPlsqlStoredProcedure(procedureName.getName(), procedureName.getCtlId(), procedureName.getDbId());
        } catch (Exception e) {
            throw new RuntimeException("failed to remove procedure", e);
        }
    }

    private boolean isCached(String name) {
        return cache.containsKey(qualified(name));
    }

    @Override
    public void removeCached(String name) {
        cache.remove(qualified(name));
    }

    private String qualified(String name) {
        return (ConnectContext.get().getDatabase() + "." + name).toUpperCase();
    }

    private String getDbName(long catalogId, long dbId) {
        String dbName = "";
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (catalog != null) {
            DatabaseIf db = catalog.getDbNullable(dbId);
            if (db != null) {
                dbName = db.getFullName();
            }
        }
        return dbName;
    }

    public boolean like(String str, String wild) {
        str = str.toLowerCase();
        return str.matches(wild.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    public boolean applyFilter(String value, String filter) {
        if (filter.isEmpty()) {
            return true;
        }
        return like(value, filter);
    }

    @Override
    public void showProcedure(List<List<String>> columns, String dbFilter, String procFilter) {
        Map<PlsqlProcedureKey, PlsqlStoredProcedure> allProc = client.getAllPlsqlStoredProcedures();
        for (Map.Entry<PlsqlProcedureKey, PlsqlStoredProcedure> entry : allProc.entrySet()) {
            List<String> row = new ArrayList<>();
            PlsqlStoredProcedure proc = entry.getValue();
            if (!applyFilter(proc.getName(), procFilter)) {
                continue;
            }
            String dbName = getDbName(proc.getCatalogId(), proc.getDbId());
            if (!applyFilter(dbName, dbFilter)) {
                continue;
            }
            row.add(proc.getName());
            row.add(Long.toString(proc.getCatalogId()));
            row.add(Long.toString(proc.getDbId()));
            row.add(dbName);
            row.add(proc.getPackageName());
            row.add(proc.getOwnerName());
            row.add(proc.getCreateTime());
            row.add(proc.getModifyTime());
            row.add(proc.getSource());
            columns.add(row);
        }
    }

    @Override
    public void showCreateProcedure(FuncNameInfo procedureName, List<List<String>> columns) {
        List<String> row = new ArrayList<>();
        PlsqlStoredProcedure proc = client.getPlsqlStoredProcedure(procedureName.getName(),
                                                                procedureName.getCtlId(), procedureName.getDbId());
        if (proc != null) {
            row.add(proc.getName());
            row.add(proc.getSource());
            columns.add(row);
        }
    }

    @Override
    public boolean exec(FuncNameInfo procedureName, Expr_func_paramsContext ctx) {
        if (builtinFunctions.exec(procedureName.toString(), ctx)) { // First look for built-in functions.
            return true;
        }
        if (isCached(procedureName.toString())) {
            trace(ctx, "EXEC CACHED FUNCTION " + procedureName);
            execProcOrFunc(ctx, cache.get(qualified(procedureName.toString())), procedureName.toString());
            return true;
        }
        Optional<PlsqlStoredProcedure> proc = getProc(procedureName);
        if (proc.isPresent()) {
            trace(ctx, "EXEC HMS FUNCTION " + procedureName);
            ParserRuleContext procCtx = parse(proc.get());
            execProcOrFunc(ctx, procCtx, procedureName.toString());
            saveInCache(procedureName.toString(), procCtx);
            return true;
        }
        return false;
    }

    /**
     * Execute a stored procedure using CALL or EXEC statement passing parameters
     */
    private void execProcOrFunc(Expr_func_paramsContext ctx, ParserRuleContext procCtx, String name) {
        exec.callStackPush(name);
        HashMap<String, Var> out = new HashMap<>();
        ArrayList<Var> actualParams = getActualCallParameters(ctx);
        exec.enterScope(Scope.Type.ROUTINE);
        callWithParameters(ctx, procCtx, out, actualParams);
        exec.callStackPop();
        exec.leaveScope();
        for (Map.Entry<String, Var> i : out.entrySet()) { // Set OUT parameters
            exec.setVariable(i.getKey(), i.getValue());
        }
    }

    private void callWithParameters(Expr_func_paramsContext ctx, ParserRuleContext procCtx, HashMap<String, Var> out,
            ArrayList<Var> actualParams) {
        if (procCtx instanceof Create_function_stmtContext) {
            Create_function_stmtContext func = (Create_function_stmtContext) procCtx;
            InMemoryFunctionRegistry.setCallParameters(func.multipartIdentifier().getText(), ctx, actualParams,
                    func.create_routine_params(), null, exec);
            if (func.declare_block_inplace() != null) {
                exec.visit(func.declare_block_inplace());
            }
            exec.visit(func.single_block_stmt());
        } else {
            Create_procedure_stmtContext proc = (Create_procedure_stmtContext) procCtx;
            InMemoryFunctionRegistry.setCallParameters(proc.multipartIdentifier().getText(), ctx, actualParams,
                    proc.create_routine_params(), out, exec);
            exec.visit(proc.procedure_block());
        }
    }

    private ParserRuleContext parse(PlsqlStoredProcedure proc) {
        PLLexer lexer = new PLLexer(new CaseInsensitiveStream(CharStreams.fromString(proc.getSource())));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PLParser parser = new PLParser(tokens);
        ProcedureVisitor visitor = new ProcedureVisitor();
        parser.program().accept(visitor);
        return visitor.func != null ? visitor.func : visitor.proc;
    }

    private Optional<PlsqlStoredProcedure> getProc(FuncNameInfo procedureName) {
        return Optional.ofNullable(client.getPlsqlStoredProcedure(procedureName.getName(), procedureName.getCtlId(),
                procedureName.getDbId()));
    }

    private ArrayList<Var> getActualCallParameters(Expr_func_paramsContext actual) {
        if (actual == null || actual.func_param() == null) {
            return null;
        }
        int cnt = actual.func_param().size();
        ArrayList<Var> values = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            values.add(evalPop(actual.func_param(i).expr()));
        }
        return values;
    }

    @Override
    public void addUserFunction(Create_function_stmtContext ctx) {
        FuncNameInfo procedureName = new FuncNameInfo(
                exec.logicalPlanBuilder.visitMultipartIdentifier(ctx.multipartIdentifier()));
        if (builtinFunctions.exists(procedureName.toString())) {
            exec.info(ctx, procedureName.toString() + " is a built-in function which cannot be redefined.");
            return;
        }
        trace(ctx, "CREATE FUNCTION " + procedureName.toString());
        saveInCache(procedureName.toString(), ctx);
        save(procedureName, Exec.getFormattedText(ctx), ctx.REPLACE() != null);
    }

    @Override
    public void addUserProcedure(Create_procedure_stmtContext ctx) {
        FuncNameInfo procedureName = new FuncNameInfo(
                exec.logicalPlanBuilder.visitMultipartIdentifier(ctx.multipartIdentifier()));
        if (builtinFunctions.exists(procedureName.toString())) {
            exec.info(ctx, procedureName.toString() + " is a built-in function which cannot be redefined.");
            return;
        }
        trace(ctx, "CREATE PROCEDURE " + procedureName.toString());
        saveInCache(procedureName.toString(), ctx);
        save(procedureName, Exec.getFormattedText(ctx), ctx.REPLACE() != null);
    }

    @Override
    public void save(FuncNameInfo procedureName, String source, boolean isForce) {
        try {
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String createTime = f.format(Calendar.getInstance().getTime());
            String modifyTime = createTime;
            if (isForce) {
                // need to get create time and use that.
                PlsqlStoredProcedure oldProc = client.getPlsqlStoredProcedure(procedureName.getName(),
                                                        procedureName.getCtlId(), procedureName.getDbId());
                if (oldProc != null) {
                    createTime = oldProc.getCreateTime();
                }
            }
            // TODO support packageName
            client.addPlsqlStoredProcedure(procedureName.getName(), procedureName.getCtlId(), procedureName.getDbId(),
                    "",
                    ConnectContext.get().getQualifiedUser(), source, createTime, modifyTime, isForce);
        } catch (Exception e) {
            throw new RuntimeException("failed to save procedure", e);
        }
    }

    private void saveInCache(String name, ParserRuleContext procCtx) {
        // TODO, removeCached needs to be synchronized to all Observer FEs.
        // Even if it is always executed on the Master FE, it still has to deal with Master switching.
        // cache.put(qualified(name.toUpperCase()), procCtx);
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    private Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        return exec.stackPop();
    }

    private void trace(ParserRuleContext ctx, String message) {
        if (trace) {
            exec.trace(ctx, message);
        }
    }

    private static class ProcedureVisitor extends PLParserBaseVisitor<Void> {
        Create_function_stmtContext func;
        Create_procedure_stmtContext proc;

        @Override
        public Void visitCreate_procedure_stmt(Create_procedure_stmtContext ctx) {
            proc = ctx;
            return null;
        }

        @Override
        public Void visitCreate_function_stmt(Create_function_stmtContext ctx) {
            func = ctx;
            return null;
        }
    }
}
