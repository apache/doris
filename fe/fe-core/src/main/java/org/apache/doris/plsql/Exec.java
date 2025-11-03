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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Exec.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.nereids.PLLexer;
import org.apache.doris.nereids.PLParser;
import org.apache.doris.nereids.PLParser.Allocate_cursor_stmtContext;
import org.apache.doris.nereids.PLParser.Assignment_stmt_collection_itemContext;
import org.apache.doris.nereids.PLParser.Assignment_stmt_multiple_itemContext;
import org.apache.doris.nereids.PLParser.Assignment_stmt_select_itemContext;
import org.apache.doris.nereids.PLParser.Assignment_stmt_single_itemContext;
import org.apache.doris.nereids.PLParser.Associate_locator_stmtContext;
import org.apache.doris.nereids.PLParser.Begin_end_blockContext;
import org.apache.doris.nereids.PLParser.Bool_exprContext;
import org.apache.doris.nereids.PLParser.Bool_expr_binaryContext;
import org.apache.doris.nereids.PLParser.Bool_expr_unaryContext;
import org.apache.doris.nereids.PLParser.Bool_literalContext;
import org.apache.doris.nereids.PLParser.Break_stmtContext;
import org.apache.doris.nereids.PLParser.Call_stmtContext;
import org.apache.doris.nereids.PLParser.Close_stmtContext;
import org.apache.doris.nereids.PLParser.Create_function_stmtContext;
import org.apache.doris.nereids.PLParser.Create_package_body_stmtContext;
import org.apache.doris.nereids.PLParser.Create_package_stmtContext;
import org.apache.doris.nereids.PLParser.Create_procedure_stmtContext;
import org.apache.doris.nereids.PLParser.Date_literalContext;
import org.apache.doris.nereids.PLParser.Dec_numberContext;
import org.apache.doris.nereids.PLParser.Declare_condition_itemContext;
import org.apache.doris.nereids.PLParser.Declare_cursor_itemContext;
import org.apache.doris.nereids.PLParser.Declare_handler_itemContext;
import org.apache.doris.nereids.PLParser.Declare_var_itemContext;
import org.apache.doris.nereids.PLParser.Doris_statementContext;
import org.apache.doris.nereids.PLParser.Drop_procedure_stmtContext;
import org.apache.doris.nereids.PLParser.DtypeContext;
import org.apache.doris.nereids.PLParser.Dtype_lenContext;
import org.apache.doris.nereids.PLParser.Exception_block_itemContext;
import org.apache.doris.nereids.PLParser.Exec_stmtContext;
import org.apache.doris.nereids.PLParser.Exit_stmtContext;
import org.apache.doris.nereids.PLParser.ExprContext;
import org.apache.doris.nereids.PLParser.Expr_agg_window_funcContext;
import org.apache.doris.nereids.PLParser.Expr_case_searchedContext;
import org.apache.doris.nereids.PLParser.Expr_case_simpleContext;
import org.apache.doris.nereids.PLParser.Expr_concatContext;
import org.apache.doris.nereids.PLParser.Expr_cursor_attributeContext;
import org.apache.doris.nereids.PLParser.Expr_dot_method_callContext;
import org.apache.doris.nereids.PLParser.Expr_dot_property_accessContext;
import org.apache.doris.nereids.PLParser.Expr_funcContext;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Expr_intervalContext;
import org.apache.doris.nereids.PLParser.Expr_spec_funcContext;
import org.apache.doris.nereids.PLParser.Expr_stmtContext;
import org.apache.doris.nereids.PLParser.Fetch_stmtContext;
import org.apache.doris.nereids.PLParser.For_cursor_stmtContext;
import org.apache.doris.nereids.PLParser.For_range_stmtContext;
import org.apache.doris.nereids.PLParser.Get_diag_stmt_exception_itemContext;
import org.apache.doris.nereids.PLParser.Get_diag_stmt_rowcount_itemContext;
import org.apache.doris.nereids.PLParser.Host_cmdContext;
import org.apache.doris.nereids.PLParser.Host_stmtContext;
import org.apache.doris.nereids.PLParser.Ident_plContext;
import org.apache.doris.nereids.PLParser.If_bteq_stmtContext;
import org.apache.doris.nereids.PLParser.If_plsql_stmtContext;
import org.apache.doris.nereids.PLParser.If_tsql_stmtContext;
import org.apache.doris.nereids.PLParser.Include_stmtContext;
import org.apache.doris.nereids.PLParser.Int_numberContext;
import org.apache.doris.nereids.PLParser.Label_stmtContext;
import org.apache.doris.nereids.PLParser.Leave_stmtContext;
import org.apache.doris.nereids.PLParser.Map_object_stmtContext;
import org.apache.doris.nereids.PLParser.MultipartIdentifierContext;
import org.apache.doris.nereids.PLParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.PLParser.Null_constContext;
import org.apache.doris.nereids.PLParser.Open_stmtContext;
import org.apache.doris.nereids.PLParser.Print_stmtContext;
import org.apache.doris.nereids.PLParser.ProgramContext;
import org.apache.doris.nereids.PLParser.QueryContext;
import org.apache.doris.nereids.PLParser.Quit_stmtContext;
import org.apache.doris.nereids.PLParser.Resignal_stmtContext;
import org.apache.doris.nereids.PLParser.Return_stmtContext;
import org.apache.doris.nereids.PLParser.Set_current_schema_optionContext;
import org.apache.doris.nereids.PLParser.Set_doris_session_optionContext;
import org.apache.doris.nereids.PLParser.Signal_stmtContext;
import org.apache.doris.nereids.PLParser.StmtContext;
import org.apache.doris.nereids.PLParser.StringContext;
import org.apache.doris.nereids.PLParser.Timestamp_literalContext;
import org.apache.doris.nereids.PLParser.Unconditional_loop_stmtContext;
import org.apache.doris.nereids.PLParser.Values_into_stmtContext;
import org.apache.doris.nereids.PLParser.While_stmtContext;
import org.apache.doris.nereids.parser.CaseInsensitiveStream;
import org.apache.doris.nereids.parser.ParserUtils;
import org.apache.doris.nereids.parser.plsql.PLSqlLogicalPlanBuilder;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.plsql.Var.Type;
import org.apache.doris.plsql.exception.PlValidationException;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.plsql.exception.TypeException;
import org.apache.doris.plsql.exception.UndefinedIdentException;
import org.apache.doris.plsql.executor.JdbcQueryExecutor;
import org.apache.doris.plsql.executor.Metadata;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;
import org.apache.doris.plsql.executor.ResultListener;
import org.apache.doris.plsql.functions.BuiltinFunctions;
import org.apache.doris.plsql.functions.DorisFunctionRegistry;
import org.apache.doris.plsql.functions.FunctionDatetime;
import org.apache.doris.plsql.functions.FunctionMisc;
import org.apache.doris.plsql.functions.FunctionRegistry;
import org.apache.doris.plsql.functions.FunctionString;
import org.apache.doris.plsql.functions.InMemoryFunctionRegistry;
import org.apache.doris.plsql.metastore.PlsqlMetaClient;
import org.apache.doris.plsql.objects.DbmOutput;
import org.apache.doris.plsql.objects.DbmOutputClass;
import org.apache.doris.plsql.objects.Method;
import org.apache.doris.plsql.objects.MethodDictionary;
import org.apache.doris.plsql.objects.MethodParams;
import org.apache.doris.plsql.objects.PlObject;
import org.apache.doris.plsql.objects.Table;
import org.apache.doris.plsql.objects.TableClass;
import org.apache.doris.plsql.objects.UtlFile;
import org.apache.doris.plsql.objects.UtlFileClass;
import org.apache.doris.plsql.packages.DorisPackageRegistry;
import org.apache.doris.plsql.packages.InMemoryPackageRegistry;
import org.apache.doris.plsql.packages.PackageRegistry;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * PL/SQL script executor
 */
public class Exec extends org.apache.doris.nereids.PLParserBaseVisitor<Integer> implements Closeable {
    private static final Logger LOG = LogManager.getLogger(Exec.class);

    public static final String VERSION = "PL/SQL 0.1";
    public static final String ERRORCODE = "ERRORCODE";
    public static final String SQLCODE = "SQLCODE";
    public static final String SQLSTATE = "SQLSTATE";
    public static final String HOSTCODE = "HOSTCODE";

    Exec exec;
    public FunctionRegistry functions;
    private BuiltinFunctions builtinFunctions;
    private PlsqlMetaClient client;
    QueryExecutor queryExecutor;
    private PackageRegistry packageRegistry = new InMemoryPackageRegistry();
    private boolean packageLoading = false;
    private final Map<String, TableClass> types = new HashMap<>();

    public enum OnError {
        EXCEPTION, SETERROR, STOP
    }

    // Scopes of execution (code blocks) with own local variables, parameters and exception handlers
    Stack<Scope> scopes = new Stack<>();
    Scope globalScope;
    Scope currentScope;

    Stack<Var> stack = new Stack<>();
    Stack<String> labels = new Stack<>();
    Stack<String> callStack = new Stack<>();

    Stack<Signal> signals = new Stack<>();
    Signal currentSignal;
    Scope currentHandlerScope;
    boolean resignal = false;

    HashMap<String, String> managedTables = new HashMap<>();
    HashMap<String, String> objectMap = new HashMap<>();
    HashMap<String, String> objectConnMap = new HashMap<>();
    HashMap<String, ArrayList<Var>> returnCursors = new HashMap<>();
    HashMap<String, Package> packages = new HashMap<>();

    Package currentPackageDecl = null;
    Arguments arguments = new Arguments();
    public Conf conf;
    Expression expr;
    Converter converter;
    Meta meta;
    Stmt stmt;
    Conn conn;
    Console console = Console.STANDARD;
    ResultListener resultListener = ResultListener.NONE;

    int rowCount = 0;

    StringBuilder localUdf = new StringBuilder();
    boolean initRoutines = false;
    public boolean inCallStmt = false;
    boolean udfRegistered = false;
    boolean udfRun = false;

    boolean dotPlsqlrcExists = false;
    boolean plsqlrcExists = false;

    boolean trace = false;
    boolean info = true;
    boolean offline = false;
    public PLSqlLogicalPlanBuilder logicalPlanBuilder;

    public Exec() {
        exec = this;
        queryExecutor = new JdbcQueryExecutor(this); // use by pl-sql.sh
    }

    public Exec(Conf conf, Console console, QueryExecutor queryExecutor, ResultListener resultListener) {
        this.conf = conf;
        this.exec = this;
        this.console = console;
        this.queryExecutor = queryExecutor;
        this.resultListener = resultListener;
        this.client = new PlsqlMetaClient();
    }

    Exec(Exec exec) {
        this.exec = exec;
        this.console = exec.console;
        this.queryExecutor = exec.queryExecutor;
        this.client = exec.client;
    }

    /**
     * Set a variable using a value from the parameter or the stack
     */
    public Var setVariable(String name, Var value) {
        if (value == null || value == Var.Empty) {
            if (exec.stack.empty()) {
                return Var.Empty;
            }
            value = exec.stack.pop();
        }
        if (name.startsWith("plsql.")) {
            exec.conf.setOption(name, value.toString());
            return Var.Empty;
        }
        Var var = findVariable(name);
        if (var != null) {
            var.cast(value);
        } else {
            var = new Var(value);
            var.setName(name);
            if (exec.currentScope != null) {
                exec.currentScope.addVariable(var);
            }
        }
        return var;
    }

    public Var setVariable(String name) {
        return setVariable(name, Var.Empty);
    }

    public Var setVariable(String name, String value) {
        return setVariable(name, new Var(value));
    }

    public Var setVariable(String name, int value) {
        return setVariable(name, new Var(Long.valueOf(value)));
    }

    /**
     * Set variable to NULL
     */
    public Var setVariableToNull(String name) {
        Var var = findVariable(name);
        if (var != null) {
            var.removeValue();
        } else {
            var = new Var();
            var.setName(name);
            if (exec.currentScope != null) {
                exec.currentScope.addVariable(var);
            }
        }
        return var;
    }

    /**
     * Add a local variable to the current scope
     */
    public void addVariable(Var var) {
        if (currentPackageDecl != null) {
            currentPackageDecl.addVariable(var);
        } else if (exec.currentScope != null) {
            exec.currentScope.addVariable(var);
        }
    }

    /**
     * Add a condition handler to the current scope
     */
    public void addHandler(Handler handler) {
        if (exec.currentScope != null) {
            exec.currentScope.addHandler(handler);
        }
    }

    /**
     * Add a return cursor visible to procedure callers and clients
     */
    public void addReturnCursor(Var var) {
        String routine = callStackPeek();
        ArrayList<Var> cursors = returnCursors.computeIfAbsent(routine, k -> new ArrayList<>());
        cursors.add(var);
    }

    /**
     * Get the return cursor defined in the specified procedure
     */
    public Var consumeReturnCursor(String routine) {
        ArrayList<Var> cursors = returnCursors.get(routine.toUpperCase());
        if (cursors == null) {
            return null;
        }
        Var var = cursors.get(0);
        cursors.remove(0);
        return var;
    }

    /**
     * Push a value to the stack
     */
    public void stackPush(Var var) {
        exec.stack.push(var);
    }

    /**
     * Push a string value to the stack
     */
    public void stackPush(String val) {
        exec.stack.push(new Var(val));
    }

    public void stackPush(StringBuilder val) {
        stackPush(val.toString());
    }

    /**
     * Push a boolean value to the stack
     */
    public void stackPush(Boolean val) {
        exec.stack.push(new Var(val));
    }

    /**
     * Select a value from the stack, but not remove
     */
    public Var stackPeek() {
        return exec.stack.peek();
    }

    /**
     * Pop a value from the stack
     */
    public Var stackPop() {
        if (!exec.stack.isEmpty()) {
            return exec.stack.pop();
        }
        return Var.Empty;
    }

    /**
     * Push a value to the call stack
     */
    public void callStackPush(String val) {
        exec.callStack.push(val.toUpperCase());
    }

    /**
     * Select a value from the call stack, but not remove
     */
    public String callStackPeek() {
        if (!exec.callStack.isEmpty()) {
            return exec.callStack.peek();
        }
        return null;
    }

    /**
     * Pop a value from the call stack
     */
    public String callStackPop() {
        if (!exec.callStack.isEmpty()) {
            return exec.callStack.pop();
        }
        return null;
    }

    /**
     * Find an existing variable by name
     */
    public Var findVariable(String name) {
        Var var;
        String name1 = name.toUpperCase();
        String name1a = null;
        String name2;
        Scope cur = exec.currentScope;
        Package pack;
        Package packCallContext = exec.getPackageCallContext();
        ArrayList<String> qualified = exec.meta.splitIdentifier(name);
        if (qualified != null) {
            name1 = qualified.get(0).toUpperCase();
            name2 = qualified.get(1).toUpperCase();
            pack = findPackage(name1);
            if (pack != null) {
                var = pack.findVariable(name2);
                if (var != null) {
                    return var;
                }
            }
        }
        if (name1.startsWith(":")) {
            name1a = name1.substring(1);
        }
        while (cur != null) {
            var = findVariable(cur.vars, name1);
            if (var == null && name1a != null) {
                var = findVariable(cur.vars, name1a);
            }
            if (var == null && packCallContext != null) {
                var = packCallContext.findVariable(name1);
            }
            if (var != null) {
                return var;
            }
            if (cur.type == Scope.Type.ROUTINE) {
                cur = exec.globalScope;
            } else {
                cur = cur.parent;
            }
        }
        return null;
    }

    public Var findVariable(Var name) {
        return findVariable(name.getName());
    }

    Var findVariable(Map<String, Var> vars, String name) {
        return vars.get(name.toUpperCase());
    }

    /**
     * Find a cursor variable by name
     */
    public Var findCursor(String name) {
        Var cursor = exec.findVariable(name);
        if (cursor != null && cursor.type == Type.CURSOR) {
            return cursor;
        }
        return null;
    }

    /**
     * Find the package by name
     */
    Package findPackage(String name) {
        Package pkg = packages.get(name.toUpperCase());
        if (pkg != null) {
            return pkg;
        }
        Optional<String> source = exec.packageRegistry.getPackage(name);
        if (source.isPresent()) {
            PLLexer lexer = new PLLexer(new CaseInsensitiveStream(CharStreams.fromString(source.get())));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PLParser parser = newParser(tokens);
            exec.packageLoading = true;
            try {
                visit(parser.program());
            } finally {
                exec.packageLoading = false;
            }
        } else {
            return null;
        }
        return packages.get(name.toUpperCase());
    }

    /**
     * Enter a new scope
     */
    public void enterScope(Scope scope) {
        exec.scopes.push(scope);
    }

    public void enterScope(Scope.Type type) {
        enterScope(type, null);
    }

    public void enterScope(Scope.Type type, Package pack) {
        exec.currentScope = new Scope(exec.currentScope, type, pack);
        enterScope(exec.currentScope);
    }

    public void enterGlobalScope() {
        globalScope = new Scope(Scope.Type.GLOBAL);
        currentScope = globalScope;
        enterScope(globalScope);
    }

    /**
     * Leave the current scope
     */
    public void leaveScope() {
        if (!exec.signals.empty()) {
            Scope scope = exec.scopes.peek();
            Signal signal = exec.signals.peek();
            if (exec.conf.onError != OnError.SETERROR) {
                runExitHandler();
            }
            if (signal.type == Signal.Type.LEAVE_ROUTINE && scope.type == Scope.Type.ROUTINE) {
                exec.signals.pop();
            }
        }
        exec.currentScope = exec.scopes.pop().getParent();
    }

    /**
     * Send a signal
     */
    public void signal(Signal signal) {
        exec.signals.push(signal);
    }

    public void signal(Signal.Type type, String value, Exception exception) {
        signal(new Signal(type, value, exception));
    }

    public void signal(Signal.Type type, String value) {
        setSqlCode(SqlCodes.ERROR);
        signal(type, value, null);
    }

    public void signal(Signal.Type type) {
        setSqlCode(SqlCodes.ERROR);
        signal(type, null, null);
    }

    public void signal(Query query) {
        setSqlCode(query.getException());
        signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.getException());
    }

    public void signal(QueryResult query) {
        setSqlCode(query.exception());
        signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.exception());
    }

    public void signal(Exception exception) {
        setSqlCode(exception);
        signal(Signal.Type.SQLEXCEPTION, exception.getMessage(), exception);
    }

    /**
     * Resignal the condition
     */
    public void resignal() {
        resignal(exec.currentSignal);
    }

    public void resignal(Signal signal) {
        if (signal != null) {
            exec.resignal = true;
            signal(signal);
        }
    }

    /**
     * Run CONTINUE handlers
     */
    boolean runContinueHandler() {
        Scope cur = exec.currentScope;
        exec.currentSignal = exec.signals.pop();
        while (cur != null) {
            for (Handler h : cur.handlers) {
                if (h.execType != Handler.ExecType.CONTINUE) {
                    continue;
                }
                if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type)
                        || (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type
                        && h.value.equalsIgnoreCase(exec.currentSignal.value))) {
                    trace(h.ctx, "CONTINUE HANDLER");
                    enterScope(Scope.Type.HANDLER);
                    exec.currentHandlerScope = h.scope;
                    visit(h.ctx.single_block_stmt());
                    leaveScope();
                    exec.currentSignal = null;
                    return true;
                }
            }
            cur = cur.parent;
        }
        exec.signals.push(exec.currentSignal);
        exec.currentSignal = null;
        return false;
    }

    /**
     * Run EXIT handler defined for the current scope
     */
    boolean runExitHandler() {
        exec.currentSignal = exec.signals.pop();
        for (Handler h : currentScope.handlers) {
            if (h.execType != Handler.ExecType.EXIT) {
                continue;
            }
            if ((h.type != Signal.Type.USERDEFINED && h.type == exec.currentSignal.type)
                    || (h.type == Signal.Type.USERDEFINED && h.type == exec.currentSignal.type
                    && h.value.equalsIgnoreCase(currentSignal.value))) {
                trace(h.ctx, "EXIT HANDLER");
                enterScope(Scope.Type.HANDLER);
                exec.currentHandlerScope = h.scope;
                visit(h.ctx.single_block_stmt());
                leaveScope();
                exec.currentSignal = null;
                return true;
            }
        }
        exec.signals.push(exec.currentSignal);
        exec.currentSignal = null;
        return false;
    }

    /**
     * Pop the last signal
     */
    public Signal signalPop() {
        if (!exec.signals.empty()) {
            return exec.signals.pop();
        }
        return null;
    }

    /**
     * Peek the last signal
     */
    public Signal signalPeek() {
        if (!exec.signals.empty()) {
            return exec.signals.peek();
        }
        return null;
    }

    /**
     * Pop the current label
     */
    public String labelPop() {
        if (!exec.labels.empty()) {
            return exec.labels.pop();
        }
        return "";
    }

    /**
     * Execute a SQL query (SELECT)
     */
    public Query executeQuery(ParserRuleContext ctx, Query query, String connProfile) {
        if (!exec.offline) {
            exec.rowCount = 0;
            exec.conn.executeQuery(query, connProfile);
            return query;
        }
        setSqlNoData();
        info(ctx, "Not executed - offline mode set");
        return query;
    }

    /**
     * Register JARs, FILEs and CREATE TEMPORARY FUNCTION for UDF call
     */
    public void registerUdf() {
        if (udfRegistered) {
            return;
        }
        ArrayList<String> sql = new ArrayList<>();
        String dir = Utils.getExecDir();
        String plsqlJarName = "plsql.jar";
        for (String jarName : Objects.requireNonNull(new File(dir).list())) {
            if (jarName.startsWith("doris-plsql") && jarName.endsWith(".jar")) {
                plsqlJarName = jarName;
                break;
            }
        }
        sql.add("ADD JAR " + dir + plsqlJarName);
        sql.add("ADD JAR " + dir + "antlr4-runtime-4.5.jar");
        if (dotPlsqlrcExists) {
            sql.add("ADD FILE " + dir + Conf.DOT_PLSQLRC);
        }
        if (plsqlrcExists) {
            sql.add("ADD FILE " + dir + Conf.PLSQLRC);
        }
        String lu = createLocalUdf();
        if (lu != null) {
            sql.add("ADD FILE " + lu);
        }
        sql.add("CREATE TEMPORARY FUNCTION plsql AS 'org.apache.doris.udf.plsql.Udf'");
        exec.conn.addPreSql(exec.conf.defaultConnection, sql);
        udfRegistered = true;
    }

    /**
     * Initialize options
     */
    void initOptions() {
        for (Entry<String, String> item : exec.conf) {
            String key = item.getKey();
            String value = item.getValue();
            if (key == null || value == null || !key.startsWith("plsql.")) {
                continue;
            } else if (key.compareToIgnoreCase(Conf.CONN_DEFAULT) == 0) {
                exec.conf.defaultConnection = value;
            } else if (key.startsWith("plsql.conn.init.")) {
                exec.conn.addConnectionInit(key.substring(17), value);
            } else if (key.startsWith(Conf.CONN_CONVERT)) {
                exec.conf.setConnectionConvert(key.substring(20), value);
            } else if (key.startsWith("plsql.conn.")) {
                String name = key.substring(12);
                exec.conn.addConnection(name, value);
            } else if (key.startsWith("plsql.")) {
                exec.conf.setOption(key, value);
            }
        }
    }

    /**
     * Set SQLCODE
     */
    public void setSqlCode(int sqlcode) {
        Long code = (long) sqlcode;
        Var var = findVariable(SQLCODE);
        if (var != null) {
            var.setValue(code);
        }
        var = findVariable(ERRORCODE);
        if (var != null) {
            var.setValue(code);
        }
    }

    public void setSqlCode(Exception exception) {
        if (exception instanceof QueryException) {
            setSqlCode(((QueryException) exception).getErrorCode());
            setSqlState(((QueryException) exception).getSQLState());
        } else {
            setSqlCode(SqlCodes.ERROR);
            setSqlState("02000");
        }
    }

    /**
     * Set SQLSTATE
     */
    public void setSqlState(String sqlstate) {
        Var var = findVariable(SQLSTATE);
        if (var != null) {
            var.setValue(sqlstate);
        }
    }

    public void setResultListener(ResultListener resultListener) {
        stmt.setResultListener(resultListener);
    }

    /**
     * Set HOSTCODE
     */
    public void setHostCode(int code) {
        Var var = findVariable(HOSTCODE);
        if (var != null) {
            var.setValue(Long.valueOf(code));
        }
    }

    /**
     * Set successful execution for SQL
     */
    public void setSqlSuccess() {
        setSqlCode(SqlCodes.SUCCESS);
        setSqlState("00000");
    }

    /**
     * Set SQL_NO_DATA as the result of SQL execution
     */
    public void setSqlNoData() {
        setSqlCode(SqlCodes.NO_DATA_FOUND);
        setSqlState("01000");
    }

    public Integer run(String[] args) throws Exception {
        if (!parseArguments(args)) {
            return -1;
        }
        init();
        try {
            parseAndEval(arguments);
        } catch (Exception e) {
            exec.signal(e);
        } finally {
            close();
        }
        return getProgramReturnCode();
    }

    public Var parseAndEval(Arguments arguments) throws IOException {
        ParseTree tree;
        try {
            CharStream input = sourceStream(arguments);
            tree = parse(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Var result = null;
        try {
            result = evaluate(tree, arguments.main);
        } catch (PlValidationException e) {
            signal(Signal.Type.VALIDATION, e.getMessage(), e);
        }
        if (result != null) {
            console.printLine(result.toString());
        }
        return result;
    }

    private Var evaluate(ParseTree tree, String execMain) {
        if (tree == null) {
            return null;
        }
        if (execMain != null) {
            initRoutines = true;
            visit(tree);
            initRoutines = false;
            exec.functions.exec(new FuncNameInfo(execMain), null);
        } else {
            visit(tree);
        }
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return null;
    }

    @Override
    public void close() {
        leaveScope();
        cleanup();
        printExceptions();
    }

    private CharStream sourceStream(Arguments arguments) throws IOException {
        return arguments.execString != null
                ? CharStreams.fromString(arguments.execString)
                : CharStreams.fromFileName(arguments.fileName);
    }

    /**
     * Initialize PL/HQL
     */
    public void init() {
        enterGlobalScope();
        if (conf == null) {
            conf = new Conf();
        }
        conf.init();
        conn = new Conn(this);
        meta = new Meta(this, queryExecutor);
        initOptions();
        logicalPlanBuilder = new PLSqlLogicalPlanBuilder();

        expr = new Expression(this);
        stmt = new Stmt(this, queryExecutor);
        stmt.setResultListener(resultListener);
        converter = new Converter(this);

        builtinFunctions = new BuiltinFunctions(this, queryExecutor);
        new FunctionDatetime(this, queryExecutor).register(builtinFunctions);
        new FunctionMisc(this, queryExecutor).register(builtinFunctions);
        new FunctionString(this, queryExecutor).register(builtinFunctions);
        if (client != null) {
            functions = new DorisFunctionRegistry(this, client, builtinFunctions);
            packageRegistry = new DorisPackageRegistry(client);
        } else {
            functions = new InMemoryFunctionRegistry(this, builtinFunctions);
        }
        addVariable(new Var(ERRORCODE, Var.Type.BIGINT, 0L));
        addVariable(new Var(SQLCODE, Var.Type.BIGINT, 0L));
        addVariable(new Var(SQLSTATE, Var.Type.STRING, "00000"));
        addVariable(new Var(HOSTCODE, Var.Type.BIGINT, 0L));
        for (Map.Entry<String, String> v : arguments.getVars().entrySet()) {
            addVariable(new Var(v.getKey(), Var.Type.STRING, v.getValue()));
        }
        includeRcFile();
        registerBuiltins();
    }

    private ParseTree parse(CharStream input) throws IOException {
        PLLexer lexer = new PLLexer(new CaseInsensitiveStream(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PLParser parser = newParser(tokens);
        ParseTree tree = parser.program();
        if (trace) {
            console.printLine("Parser tree: " + tree.toStringTree(parser));
        }
        return tree;
    }

    protected void registerBuiltins() {
        Var dbmVar = new Var(Type.PL_OBJECT, "DBMS_OUTPUT");
        DbmOutput dbms = DbmOutputClass.INSTANCE.newInstance();
        dbms.initialize(console);
        dbmVar.setValue(dbms);
        dbmVar.setConstant(true);
        addVariable(dbmVar);

        Var utlFileVar = new Var(Type.PL_OBJECT, "UTL_FILE");
        UtlFile utlFile = UtlFileClass.INSTANCE.newInstance();
        utlFileVar.setValue(utlFile);
        utlFileVar.setConstant(true);
        addVariable(utlFileVar);
    }

    private PLParser newParser(CommonTokenStream tokens) {
        PLParser parser = new PLParser(tokens);
        // the default listener logs into stdout, overwrite it with a custom listener that uses beeline console
        parser.removeErrorListeners();
        parser.addErrorListener(new SyntaxErrorReporter(console));
        return parser;
    }

    /**
     * Parse command line arguments
     */
    boolean parseArguments(String[] args) {
        boolean parsed = arguments.parse(args);
        if (parsed && arguments.hasVersionOption()) {
            console.printLine(VERSION);
            return false;
        }
        if (!parsed || arguments.hasHelpOption()
                || (arguments.getExecString() == null && arguments.getFileName() == null)) {
            arguments.printHelp();
            return false;
        }
        String execString = arguments.getExecString();
        String execFile = arguments.getFileName();
        if (arguments.hasTraceOption()) {
            trace = true;
        }
        if (arguments.hasOfflineOption()) {
            offline = true;
        }
        if (execString != null && execFile != null) {
            console.printError("The '-e' and '-f' options cannot be specified simultaneously.");
            return false;
        }
        return true;
    }

    /**
     * Include statements from .plsqlrc and plsql rc files
     */
    void includeRcFile() {
        if (includeFile(Conf.DOT_PLSQLRC, false)) {
            dotPlsqlrcExists = true;
        } else {
            if (includeFile(Conf.PLSQLRC, false)) {
                plsqlrcExists = true;
            }
        }
        if (udfRun) {
            includeFile(Conf.PLSQL_LOCALS_SQL, true);
        }
    }

    /**
     * Include statements from a file
     */
    boolean includeFile(String file, boolean showError) {
        try {
            String content = FileUtils.readFileToString(new java.io.File(file), "UTF-8");
            if (content != null && !content.isEmpty()) {
                if (trace) {
                    trace(null, "INCLUDE CONTENT " + file + " (non-empty)");
                }
                new Exec(this).include(content);
                return true;
            }
        } catch (Exception e) {
            if (showError) {
                error(null, "INCLUDE file error: " + e.getMessage());
            }
        }
        return false;
    }

    /**
     * Execute statements from an include file
     */
    void include(String content) throws Exception {
        InputStream input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        PLLexer lexer = new PLLexer(new ANTLRInputStream(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PLParser parser = newParser(tokens);
        ParseTree tree = parser.program();
        visit(tree);
    }

    /**
     * Start executing HPL/SQL script
     */
    @Override
    public Integer visitProgram(ProgramContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * Enter BEGIN-END block
     */
    @Override
    public Integer visitBegin_end_block(Begin_end_blockContext ctx) {
        enterScope(Scope.Type.BEGIN_END);
        Integer rc = visitChildren(ctx);
        leaveScope();
        return rc;
    }

    /**
     * Free resources before exit
     */
    void cleanup() {
        for (Map.Entry<String, String> i : managedTables.entrySet()) {
            String sql = "DROP TABLE IF EXISTS " + i.getValue();
            QueryResult query = queryExecutor.executeQuery(sql, null);
            query.close();
            if (trace) {
                trace(null, sql);
            }
        }
    }

    /**
     * Output information about unhandled exceptions
     */
    public void printExceptions() {
        while (!signals.empty()) {
            Signal sig = signals.pop();
            if (sig.type == Signal.Type.VALIDATION) {
                error(((PlValidationException) sig.exception).getCtx(), sig.exception.getMessage());
            } else if (sig.type == Signal.Type.SQLEXCEPTION) {
                LOG.warn(ExceptionUtils.getStackTrace(sig.exception));
                console.printError(ErrorCode.ERR_SP_BAD_SQLSTATE,
                        "Unhandled exception in PL/SQL. " + sig.exception.toString());
            } else if (sig.type == Signal.Type.UNSUPPORTED_OPERATION) {
                console.printError(ErrorCode.ERR_SP_BAD_SQLSTATE,
                        sig.value == null ? "Unsupported operation" : sig.value);
            } else if (sig.type == Signal.Type.TOO_MANY_ROWS) {
                console.printError(ErrorCode.ERR_SP_BAD_SQLSTATE,
                        sig.value == null ? "Too many rows exception" : sig.value);
            } else if (sig.type == Signal.Type.NOTFOUND) {
                console.printError(ErrorCode.ERR_SP_FETCH_NO_DATA,
                        sig.value == null ? "Not found data exception" : sig.value);
            } else if (sig.exception != null) {
                LOG.warn(ExceptionUtils.getStackTrace(sig.exception));
                console.printError(ErrorCode.ERR_SP_BAD_SQLSTATE,
                        "PL/SQL error: " + sig.exception.toString());
            } else if (sig.value != null) {
                console.printError(ErrorCode.ERR_SP_BAD_SQLSTATE, sig.value);
            } else {
                trace(null, "Signal: " + sig.type);
            }
        }
    }

    /**
     * Get the program return code
     */
    Integer getProgramReturnCode() {
        int rc = 0;
        if (!signals.empty()) {
            Signal sig = signals.pop();
            if ((sig.type == Signal.Type.LEAVE_PROGRAM || sig.type == Signal.Type.LEAVE_ROUTINE)
                    && sig.value != null) {
                try {
                    rc = Integer.parseInt(sig.value);
                } catch (NumberFormatException e) {
                    rc = 1;
                }
            }
        }
        return rc;
    }

    /**
     * Executing a statement
     */
    @Override
    public Integer visitStmt(StmtContext ctx) {
        if (ctx.semicolon_stmt() != null) {
            return 0;
        }
        if (initRoutines && ctx.create_procedure_stmt() == null && ctx.create_function_stmt() == null) {
            return 0;
        }
        if (exec.resignal) {
            if (exec.currentScope != exec.currentHandlerScope.parent) {
                return 0;
            }
            exec.resignal = false;
        }
        if (!exec.signals.empty() && exec.conf.onError != OnError.SETERROR) {
            if (!runContinueHandler()) {
                return 0;
            }
        }
        Var prev = stackPop();
        if (prev != null && prev.value != null) {
            console.printLine(prev.toString());
        }
        return visitChildren(ctx);
    }

    @Override
    public Integer visitDoris_statement(Doris_statementContext ctx) {
        Integer rc = exec.stmt.statement(ctx);
        // Sometimes the query results are not returned to the mysql client,
        // such as `declare result; select … into result;`, not need finalize.
        resultListener.onFinalize();
        return rc;
    }

    /**
     * Executing SELECT statement
     */
    @Override
    public Integer visitQuery(QueryContext ctx) {
        return exec.stmt.statement(ctx);
    }

    /**
     * EXCEPTION block
     */
    @Override
    public Integer visitException_block_item(Exception_block_itemContext ctx) {
        if (exec.signals.empty()) {
            return 0;
        }
        if (exec.conf.onError == OnError.SETERROR || exec.conf.onError == OnError.STOP) {
            exec.signals.pop();
            return 0;
        }
        if (ctx.IDENTIFIER().toString().equalsIgnoreCase("OTHERS")) {
            trace(ctx, "EXCEPTION HANDLER");
            exec.signals.pop();
            enterScope(Scope.Type.HANDLER);
            visit(ctx.block());
            leaveScope();
        }
        return 0;
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(ImmutableList.toImmutableList());
    }

    public List<NamedExpression> getNamedExpressions(NamedExpressionSeqContext namedCtx) {
        return ParserUtils.withOrigin(namedCtx, () -> visit(namedCtx.namedExpression(), NamedExpression.class));
    }

    /**
     * DECLARE variable statement
     */
    @Override
    public Integer visitDeclare_var_item(Declare_var_itemContext ctx) {
        String type = null;
        TableClass userDefinedType = null;
        Row row = null;
        String len = null;
        String scale = null;
        Var defaultVar = null;
        if (ctx.dtype().ROWTYPE() != null) {
            row = meta.getRowDataType(ctx, exec.conf.defaultConnection, ctx.dtype().qident().getText());
            if (row == null) {
                type = Var.DERIVED_ROWTYPE;
            }
        } else {
            type = getDataType(ctx);
            if (ctx.dtype_len() != null) {
                len = ctx.dtype_len().INTEGER_VALUE(0).getText();
                if (ctx.dtype_len().INTEGER_VALUE(1) != null) {
                    scale = ctx.dtype_len().INTEGER_VALUE(1).getText();
                }
            }
            if (ctx.dtype_default() != null) {
                defaultVar = evalPop(ctx.dtype_default());
            }
            userDefinedType = types.get(type);
            if (userDefinedType != null) {
                type = Type.PL_OBJECT.name();
            }

        }
        int cnt = ctx.ident_pl().size();        // Number of variables declared with the same data type and default
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident_pl(i).getText();
            if (row == null) {
                Var var = new Var(name, type, len, scale, defaultVar);
                if (userDefinedType != null && defaultVar == null) {
                    var.setValue(userDefinedType.newInstance());
                }
                exec.addVariable(var);
                if (ctx.CONSTANT() != null) {
                    var.setConstant(true);
                }
                if (trace) {
                    if (defaultVar != null) {
                        trace(ctx, "DECLARE " + name + " " + type + " = " + var.toSqlString());
                    } else {
                        trace(ctx, "DECLARE " + name + " " + type);
                    }
                }
            } else {
                exec.addVariable(new Var(name, row));
                if (trace) {
                    trace(ctx, "DECLARE " + name + " " + ctx.dtype().getText());
                }
            }
        }
        return 0;
    }

    /**
     * Get the variable data type
     */
    String getDataType(Declare_var_itemContext ctx) {
        String type;
        if (ctx.dtype().TYPE() != null) {
            type = meta.getDataType(ctx, exec.conf.defaultConnection, ctx.dtype().qident().getText());
            if (type == null) {
                type = Var.DERIVED_TYPE;
            }
        } else {
            type = getFormattedText(ctx.dtype());
        }
        return type;
    }

    /**
     * ALLOCATE CURSOR statement
     */
    @Override
    public Integer visitAllocate_cursor_stmt(Allocate_cursor_stmtContext ctx) {
        return exec.stmt.allocateCursor(ctx);
    }

    /**
     * ASSOCIATE LOCATOR statement
     */
    @Override
    public Integer visitAssociate_locator_stmt(Associate_locator_stmtContext ctx) {
        return exec.stmt.associateLocator(ctx);
    }

    /**
     * DECLARE cursor statement
     */
    @Override
    public Integer visitDeclare_cursor_item(Declare_cursor_itemContext ctx) {
        return exec.stmt.declareCursor(ctx);
    }

    /**
     * OPEN cursor statement
     */
    @Override
    public Integer visitOpen_stmt(Open_stmtContext ctx) {
        return exec.stmt.open(ctx);
    }

    /**
     * FETCH cursor statement
     */
    @Override
    public Integer visitFetch_stmt(Fetch_stmtContext ctx) {
        return exec.stmt.fetch(ctx);
    }

    /**
     * CLOSE cursor statement
     */
    @Override
    public Integer visitClose_stmt(Close_stmtContext ctx) {
        return exec.stmt.close(ctx);
    }

    /**
     * DECLARE HANDLER statement
     */
    @Override
    public Integer visitDeclare_handler_item(Declare_handler_itemContext ctx) {
        trace(ctx, "DECLARE HANDLER");
        Handler.ExecType execType = Handler.ExecType.EXIT;
        Signal.Type type = Signal.Type.SQLEXCEPTION;
        String value = null;
        if (ctx.CONTINUE() != null) {
            execType = Handler.ExecType.CONTINUE;
        }
        if (ctx.ident_pl() != null) {
            type = Signal.Type.USERDEFINED;
            value = ctx.ident_pl().getText();
        } else if (ctx.NOT() != null && ctx.FOUND() != null) {
            type = Signal.Type.NOTFOUND;
        }
        addHandler(new Handler(execType, type, value, exec.currentScope, ctx));
        return 0;
    }

    /**
     * DECLARE CONDITION
     */
    @Override
    public Integer visitDeclare_condition_item(Declare_condition_itemContext ctx) {
        return 0;
    }

    /**
     * CREATE FUNCTION statement
     */
    @Override
    public Integer visitCreate_function_stmt(Create_function_stmtContext ctx) {
        exec.functions.addUserFunction(ctx);
        addLocalUdf(ctx);
        return 0;
    }

    /**
     * CREATE PACKAGE specification statement
     */
    @Override
    public Integer visitCreate_package_stmt(Create_package_stmtContext ctx) {
        FuncNameInfo procedureName = new FuncNameInfo(
                exec.logicalPlanBuilder.visitMultipartIdentifier(ctx.multipartIdentifier()));
        if (exec.packageLoading) {
            exec.currentPackageDecl = new Package(procedureName.toString(), exec, builtinFunctions);
            exec.packages.put(procedureName.toString(), exec.currentPackageDecl);
            exec.currentPackageDecl.createSpecification(ctx);
            exec.currentPackageDecl = null;
        } else {
            trace(ctx, "CREATE PACKAGE");
            exec.packages.remove(procedureName.toString());
            exec.packageRegistry.createPackageHeader(procedureName.toString(), getFormattedText(ctx),
                    ctx.REPLACE() != null);
        }
        return 0;
    }

    /**
     * CREATE PACKAGE body statement
     */
    @Override
    public Integer visitCreate_package_body_stmt(
            Create_package_body_stmtContext ctx) {
        FuncNameInfo procedureName = new FuncNameInfo(
                exec.logicalPlanBuilder.visitMultipartIdentifier(ctx.multipartIdentifier()));
        if (exec.packageLoading) {
            exec.currentPackageDecl = exec.packages.get(procedureName.toString());
            if (exec.currentPackageDecl == null) {
                exec.currentPackageDecl = new Package(procedureName.toString(), exec, builtinFunctions);
                exec.currentPackageDecl.setAllMembersPublic(true);
                exec.packages.put(procedureName.toString(), exec.currentPackageDecl);
            }
            exec.currentPackageDecl.createBody(ctx);
            exec.currentPackageDecl = null;
        } else {
            trace(ctx, "CREATE PACKAGE BODY");
            exec.packages.remove(procedureName.toString());
            exec.packageRegistry.createPackageBody(procedureName.toString(), getFormattedText(ctx),
                    ctx.REPLACE() != null);
        }
        return 0;
    }

    /**
     * CREATE PROCEDURE statement
     */
    @Override
    public Integer visitCreate_procedure_stmt(Create_procedure_stmtContext ctx) {
        exec.functions.addUserProcedure(ctx);
        addLocalUdf(ctx);                      // Add procedures as they can be invoked by functions
        return 0;
    }

    /**
     * Add functions and procedures defined in the current script
     */
    void addLocalUdf(ParserRuleContext ctx) {
        if (exec == this) {
            localUdf.append(Exec.getFormattedText(ctx));
            localUdf.append("\n");
        }
    }

    /**
     * Save local functions and procedures to a file (will be added to the distributed cache)
     */
    String createLocalUdf() {
        if (localUdf.length() == 0) {
            return null;
        }
        try {
            String file = System.getProperty("user.dir") + "/" + Conf.PLSQL_LOCALS_SQL;
            PrintWriter writer = new PrintWriter(file, "UTF-8");
            writer.print(localUdf);
            writer.close();
            return file;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * DROP PROCEDURE statement
     */
    @Override
    public Integer visitDrop_procedure_stmt(Drop_procedure_stmtContext ctx) {
        FuncNameInfo procedureName = new FuncNameInfo(
                exec.logicalPlanBuilder.visitMultipartIdentifier(ctx.multipartIdentifier()));
        if (builtinFunctions.exists(procedureName.toString())) {
            exec.info(ctx, procedureName.toString() + " is a built-in function which cannot be removed.");
            return 0;
        }
        if (trace) {
            trace(ctx, "DROP PROCEDURE " + procedureName.toString());
        }
        exec.functions.remove(procedureName);
        removeLocalUdf(ctx);
        return 0;
    }

    /**
     * Remove functions and procedures defined in the current script
     */
    void removeLocalUdf(ParserRuleContext ctx) {
        if (exec == this) {
            String str = Exec.getFormattedText(ctx);
            int i = localUdf.indexOf(str);
            if (i != -1) {
                localUdf.delete(i, i + str.length());
            }
        }
    }

    @Override
    public Integer visitSet_doris_session_option(
            Set_doris_session_optionContext ctx) {
        StringBuilder sql = new StringBuilder("set ");
        for (int i = 0; i < ctx.getChildCount(); i++) {
            sql.append(ctx.getChild(i).getText()).append(" ");
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx); // Send to doris for execution
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        if (trace) {
            trace(ctx, sql.toString());
        }
        return 0;
    }

    /**
     * Assignment statement for single value
     */
    @Override
    public Integer visitAssignment_stmt_single_item(
            Assignment_stmt_single_itemContext ctx) {
        String name = ctx.ident_pl().getText();
        visit(ctx.expr());
        Var var = setVariable(name);
        if (trace) {
            trace(ctx, "SET " + name + " = " + var.toSqlString());
        }
        return 0;
    }

    /**
     * Assignment statement for multiple values
     */
    @Override
    public Integer visitAssignment_stmt_multiple_item(
            Assignment_stmt_multiple_itemContext ctx) {
        int cnt = ctx.ident_pl().size();
        int ecnt = ctx.expr().size();
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident_pl(i).getText();
            if (i < ecnt) {
                visit(ctx.expr(i));
                Var var = setVariable(name);
                if (trace) {
                    trace(ctx, "SET " + name + " = " + var.toString());
                }
            }
        }
        return 0;
    }

    /**
     * Assignment from SELECT statement
     */
    @Override
    public Integer visitAssignment_stmt_select_item(
            Assignment_stmt_select_itemContext ctx) {
        return stmt.assignFromSelect(ctx);
    }

    @Override
    public Integer visitAssignment_stmt_collection_item(
            Assignment_stmt_collection_itemContext ctx) {
        Expr_funcContext lhs = ctx.expr_func();
        Var var = findVariable(lhs.multipartIdentifier().getText());
        if (var == null || var.type != Type.PL_OBJECT) {
            stackPush(Var.Null);
            return 0;
        }
        MethodParams.Arity.UNARY.check(lhs.multipartIdentifier().getText(), lhs.expr_func_params().func_param());
        Var index = evalPop(lhs.expr_func_params().func_param(0));
        Var value = evalPop(ctx.expr());
        dispatch(ctx, (PlObject) var.value, MethodDictionary.__SETITEM__, Arrays.asList(index, value));
        return 0;
    }

    /**
     * Evaluate an expression
     */
    @Override
    public Integer visitExpr(ExprContext ctx) {
        exec.expr.exec(ctx);
        return 0;
    }

    /**
     * Evaluate a boolean expression
     */
    @Override
    public Integer visitBool_expr(Bool_exprContext ctx) {
        exec.expr.execBool(ctx);
        return 0;
    }

    @Override
    public Integer visitBool_expr_binary(Bool_expr_binaryContext ctx) {
        exec.expr.execBoolBinary(ctx);
        return 0;
    }

    @Override
    public Integer visitBool_expr_unary(Bool_expr_unaryContext ctx) {
        exec.expr.execBoolUnary(ctx);
        return 0;
    }

    /**
     * Cursor attribute %ISOPEN, %FOUND and %NOTFOUND
     */
    @Override
    public Integer visitExpr_cursor_attribute(Expr_cursor_attributeContext ctx) {
        exec.expr.execCursorAttribute(ctx);
        return 0;
    }

    /**
     * Function call
     */
    @Override
    public Integer visitExpr_func(Expr_funcContext ctx) {
        return functionCall(ctx, ctx.multipartIdentifier(), ctx.expr_func_params());
    }

    private int functionCall(ParserRuleContext ctx, MultipartIdentifierContext ident,
            Expr_func_paramsContext params) {
        List<String> nameParts = logicalPlanBuilder.visitMultipartIdentifier(ident);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        Package packCallContext = exec.getPackageCallContext();
        boolean executed = false;
        Package pack = findPackage(procedureName.getDbName());
        if (pack != null) {
            executed = pack.execFunc(procedureName.getName(), params);
        }
        if (!executed && packCallContext != null) {
            executed = packCallContext.execFunc(procedureName.toString(), params);
        }
        if (!executed) {
            if (!exec.functions.exec(procedureName, params)) {
                Var var = findVariable(procedureName.toString());
                if (var != null && var.type == Type.PL_OBJECT) {
                    stackPush(dispatch(ctx, (PlObject) var.value, MethodDictionary.__GETITEM__, params));
                } else {
                    throw new UndefinedIdentException(ctx, procedureName.toString());
                }
            }
        }
        return 0;
    }

    private Var dispatch(ParserRuleContext ctx, PlObject obj, String methodName,
            Expr_func_paramsContext paramCtx) {
        List<Var> params = paramCtx == null
                ? Collections.emptyList()
                : paramCtx.func_param().stream().map(this::evalPop).collect(Collectors.toList());
        return dispatch(ctx, obj, methodName, params);
    }

    private Var dispatch(ParserRuleContext ctx, PlObject obj, String methodName, List<Var> params) {
        Method method = obj.plClass().methodDictionary().get(ctx, methodName);
        return method.call(obj, params);
    }

    /**
     * @return either 1 rowtype OR 1 single column table OR n single column tables
     */
    public List<Table> intoTables(ParserRuleContext ctx, List<String> names) {
        List<Table> tables = new ArrayList<>();
        for (String name : names) {
            Var var = findVariable(name);
            if (var == null) {
                trace(ctx, "Variable not found: " + name);
            } else if (var.type == Type.PL_OBJECT && var.value instanceof Table) {
                tables.add((Table) var.value);
            } else {
                throw new TypeException(ctx, Table.class, var.type, var.value);
            }
        }
        if (tables.size() > 1 && tables.stream().anyMatch(tbl -> tbl.plClass().rowType())) {
            throw new TypeException(ctx, "rowtype table should not be used when selecting into multiple tables");
        }
        return tables;
    }

    /**
     * Aggregate or window function call
     */
    @Override
    public Integer visitExpr_agg_window_func(Expr_agg_window_funcContext ctx) {
        exec.stackPush(Exec.getFormattedText(ctx));
        return 0;
    }

    /**
     * Function with specific syntax
     */
    @Override
    public Integer visitExpr_spec_func(Expr_spec_funcContext ctx) {
        exec.builtinFunctions.specExec(ctx);
        return 0;
    }

    /**
     * INCLUDE statement
     */
    @Override
    public Integer visitInclude_stmt(@NotNull Include_stmtContext ctx) {
        return exec.stmt.include(ctx);
    }

    /**
     * IF statement (PL/SQL syntax)
     */
    @Override
    public Integer visitIf_plsql_stmt(If_plsql_stmtContext ctx) {
        return exec.stmt.ifPlsql(ctx);
    }

    /**
     * IF statement (Transact-SQL syntax)
     */
    @Override
    public Integer visitIf_tsql_stmt(If_tsql_stmtContext ctx) {
        return exec.stmt.ifTsql(ctx);
    }

    /**
     * IF statement (BTEQ syntax)
     */
    @Override
    public Integer visitIf_bteq_stmt(If_bteq_stmtContext ctx) {
        return exec.stmt.ifBteq(ctx);
    }


    /**
     * VALUES statement
     */
    @Override
    public Integer visitValues_into_stmt(Values_into_stmtContext ctx) {
        return exec.stmt.values(ctx);
    }

    /**
     * WHILE statement
     */
    @Override
    public Integer visitWhile_stmt(While_stmtContext ctx) {
        return exec.stmt.while_(ctx);
    }

    @Override
    public Integer visitUnconditional_loop_stmt(
            Unconditional_loop_stmtContext ctx) {
        return exec.stmt.unconditionalLoop(ctx);
    }

    /**
     * FOR cursor statement
     */
    @Override
    public Integer visitFor_cursor_stmt(For_cursor_stmtContext ctx) {
        return exec.stmt.forCursor(ctx);
    }

    /**
     * FOR (integer range) statement
     */
    @Override
    public Integer visitFor_range_stmt(For_range_stmtContext ctx) {
        return exec.stmt.forRange(ctx);
    }

    /**
     * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL
     */
    @Override
    public Integer visitExec_stmt(Exec_stmtContext ctx) {
        exec.inCallStmt = true;
        Integer rc = exec.stmt.exec(ctx);
        exec.inCallStmt = false;
        return rc;
    }

    /**
     * CALL statement
     */
    @Override
    public Integer visitCall_stmt(Call_stmtContext ctx) {
        exec.inCallStmt = true;
        try {
            if (ctx.expr_func() != null) {
                functionCall(ctx, ctx.expr_func().multipartIdentifier(), ctx.expr_func().expr_func_params());
            } else if (ctx.expr_dot() != null) {
                visitExpr_dot(ctx.expr_dot());
            } else if (ctx.multipartIdentifier() != null) {
                functionCall(ctx, ctx.multipartIdentifier(), null);
            }
        } catch (Exception e) {
            exec.signal(e);
        } finally {
            exec.inCallStmt = false;
        }
        return 0;
    }

    /**
     * EXIT statement (leave the specified loop with a condition)
     */
    @Override
    public Integer visitExit_stmt(Exit_stmtContext ctx) {
        return exec.stmt.exit(ctx);
    }

    /**
     * BREAK statement (leave the innermost loop unconditionally)
     */
    @Override
    public Integer visitBreak_stmt(Break_stmtContext ctx) {
        return exec.stmt.break_(ctx);
    }

    /**
     * LEAVE statement (leave the specified loop unconditionally)
     */
    @Override
    public Integer visitLeave_stmt(Leave_stmtContext ctx) {
        return exec.stmt.leave(ctx);
    }

    /**
     * PRINT statement
     */
    @Override
    public Integer visitPrint_stmt(Print_stmtContext ctx) {
        return exec.stmt.print(ctx);
    }

    /**
     * QUIT statement
     */
    @Override
    public Integer visitQuit_stmt(Quit_stmtContext ctx) {
        return exec.stmt.quit(ctx);
    }

    /**
     * SIGNAL statement
     */
    @Override
    public Integer visitSignal_stmt(Signal_stmtContext ctx) {
        return exec.stmt.signal(ctx);
    }

    /**
     * RESIGNAL statement
     */
    @Override
    public Integer visitResignal_stmt(Resignal_stmtContext ctx) {
        return exec.stmt.resignal(ctx);
    }

    /**
     * RETURN statement
     */
    @Override
    public Integer visitReturn_stmt(Return_stmtContext ctx) {
        return exec.stmt.return_(ctx);
    }

    /**
     * SET session options
     */
    @Override
    public Integer visitSet_current_schema_option(
            Set_current_schema_optionContext ctx) {
        return exec.stmt.setCurrentSchema(ctx);
    }

    private void addType(TableClass tableClass) {
        types.put(tableClass.typeName(), tableClass);
    }

    public TableClass getType(String name) {
        return types.get(name);
    }

    /**
     * MAP OBJECT statement
     */
    @Override
    public Integer visitMap_object_stmt(Map_object_stmtContext ctx) {
        String source = ctx.ident_pl(0).getText();
        String target = null;
        String conn = null;
        if (ctx.TO() != null) {
            target = ctx.ident_pl(1).getText();
            exec.objectMap.put(source.toUpperCase(), target);
        }
        if (ctx.AT() != null) {
            if (ctx.TO() == null) {
                conn = ctx.ident_pl(1).getText();
            } else {
                conn = ctx.ident_pl(2).getText();
            }
            exec.objectConnMap.put(source.toUpperCase(), conn);
        }
        if (trace) {
            String log = "MAP OBJECT " + source;
            if (target != null) {
                log += " AS " + target;
            }
            if (conn != null) {
                log += " AT " + conn;
            }
            trace(ctx, log);
        }
        return 0;
    }

    /**
     * Executing OS command
     */
    @Override
    public Integer visitHost_cmd(Host_cmdContext ctx) {
        trace(ctx, "HOST");
        execHost(ctx, ctx.start.getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())));
        return 0;
    }

    @Override
    public Integer visitHost_stmt(Host_stmtContext ctx) {
        trace(ctx, "HOST");
        execHost(ctx, evalPop(ctx.expr()).toString());
        return 0;
    }

    public void execHost(ParserRuleContext ctx, String cmd) {
        Process p = null;
        try {
            if (trace) {
                trace(ctx, "HOST Command: " + cmd);
            }
            p = Runtime.getRuntime().exec(cmd);
            new StreamGobbler(p.getInputStream(), console).start();
            new StreamGobbler(p.getErrorStream(), console).start();
            int rc = p.waitFor();
            if (trace) {
                trace(ctx, "HOST Process exit code: " + rc);
            }
            setHostCode(rc);
        } catch (Exception e) {
            setHostCode(1);
            signal(Signal.Type.SQLEXCEPTION);
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }

    /**
     * Standalone expression (as a statement)
     */
    @Override
    public Integer visitExpr_stmt(Expr_stmtContext ctx) {
        visitChildren(ctx);
        return 0;
    }

    /**
     * String concatenation operator
     */
    @Override
    public Integer visitExpr_concat(Expr_concatContext ctx) {
        exec.expr.operatorConcat(ctx);
        return 0;
    }

    @Override
    public Integer visitExpr_dot_method_call(Expr_dot_method_callContext ctx) {
        Var var = ctx.ident_pl() != null
                ? findVariable(ctx.ident_pl().getText())
                : evalPop(ctx.expr_func(0));

        if (var == null && ctx.ident_pl() != null) {
            Package pkg = findPackage(ctx.ident_pl().getText());
            String pkgFuncName = ctx.expr_func(0).multipartIdentifier().getText().toUpperCase();
            boolean executed = pkg.execFunc(pkgFuncName, ctx.expr_func(0).expr_func_params());
            Package packCallContext = exec.getPackageCallContext();
            if (!executed && packCallContext != null) {
                packCallContext.execFunc(pkgFuncName, ctx.expr_func(0).expr_func_params());
            }
            return 0;
        }

        Expr_funcContext method = ctx.expr_func(ctx.expr_func().size() - 1);
        switch (var.type) {
            case PL_OBJECT:
                Var result = dispatch(ctx, (PlObject) var.value, method.multipartIdentifier().getText(),
                        method.expr_func_params());
                stackPush(result);
                return 0;
            default:
                throw new TypeException(ctx, var.type + " is not an object");
        }
    }

    @Override
    public Integer visitExpr_dot_property_access(
            Expr_dot_property_accessContext ctx) {
        Var var = ctx.expr_func() != null
                ? evalPop(ctx.expr_func())
                : findVariable(ctx.ident_pl(0).getText());
        String property = ctx.ident_pl(ctx.ident_pl().size() - 1).getText();

        if (var == null && ctx.expr_func() == null) {
            Package pkg = findPackage(ctx.ident_pl(0).getText());
            Var variable = pkg.findVariable(property);
            if (variable != null) {
                stackPush(variable);
            } else {
                Package packCallContext = exec.getPackageCallContext();
                stackPush(packCallContext.findVariable(property));
            }
            return 0;
        }

        switch (var.type) {
            case PL_OBJECT:
                Var result = dispatch(ctx, (PlObject) var.value, property, Collections.emptyList());
                stackPush(result);
                return 0;
            case ROW:
                stackPush(((Row) var.value).getValue(property));
                return 0;
            default:
                throw new TypeException(ctx, var.type + " is not an object/row");
        }
    }

    /**
     * Simple CASE expression
     */
    @Override
    public Integer visitExpr_case_simple(Expr_case_simpleContext ctx) {
        exec.expr.execSimpleCase(ctx);
        return 0;
    }

    /**
     * Searched CASE expression
     */
    @Override
    public Integer visitExpr_case_searched(Expr_case_searchedContext ctx) {
        exec.expr.execSearchedCase(ctx);
        return 0;
    }

    /**
     * GET DIAGNOSTICS EXCEPTION statement
     */
    @Override
    public Integer visitGet_diag_stmt_exception_item(
            Get_diag_stmt_exception_itemContext ctx) {
        return exec.stmt.getDiagnosticsException(ctx);
    }

    /**
     * GET DIAGNOSTICS ROW_COUNT statement
     */
    @Override
    public Integer visitGet_diag_stmt_rowcount_item(
            Get_diag_stmt_rowcount_itemContext ctx) {
        return exec.stmt.getDiagnosticsRowCount(ctx);
    }

    /**
     * Label
     */
    @Override
    public Integer visitLabel_stmt(Label_stmtContext ctx) {
        if (ctx.IDENTIFIER() != null) {
            exec.labels.push(ctx.IDENTIFIER().toString());
        }
        return 0;
    }

    /**
     * Identifier
     */
    @Override
    public Integer visitIdent_pl(Ident_plContext ctx) {
        boolean hasSub = false;
        String ident = ctx.getText();
        String actualIdent = ident;
        if (ident.startsWith("-")) {
            hasSub = true;
            actualIdent = ident.substring(1);
        }

        Var var = findVariable(actualIdent);
        if (var != null) { // Use previously saved variables
            if (hasSub) {
                Var var1 = new Var(var);
                var1.negate();
                exec.stackPush(var1);
            } else {
                exec.stackPush(var);
            }
        } else {
            if (exec.inCallStmt) {
                exec.stackPush(new Var(Var.Type.IDENT, ident));
            } else {
                if (!exec.functions.exec(new FuncNameInfo(ident), null)) {
                    throw new UndefinedIdentException(ctx, ident);
                }
            }
        }
        return 0;
    }

    /**
     * string literal
     */
    @Override
    public Integer visitString(StringContext ctx) {
        exec.stackPush(Utils.unquoteString(ctx.getText()));
        return 0;
    }

    /**
     * Integer literal, signed or unsigned
     */
    @Override
    public Integer visitInt_number(Int_numberContext ctx) {
        exec.stack.push(new Var(Long.valueOf(ctx.getText())));
        return 0;
    }

    /**
     * Interval expression (INTERVAL '1' DAY i.e)
     */
    @Override
    public Integer visitExpr_interval(Expr_intervalContext ctx) {
        int num = evalPop(ctx.expr()).intValue();
        Interval interval = new Interval().set(num, ctx.interval_item().getText());
        stackPush(new Var(interval));
        return 0;
    }

    /**
     * Decimal literal, signed or unsigned
     */
    @Override
    public Integer visitDec_number(Dec_numberContext ctx) {
        stackPush(new Var(new BigDecimal(ctx.getText())));
        return 0;
    }

    /**
     * Boolean literal
     */
    @Override
    public Integer visitBool_literal(Bool_literalContext ctx) {
        boolean val = true;
        if (ctx.FALSE() != null) {
            val = false;
        }
        stackPush(new Var(val));
        return 0;
    }

    /**
     * NULL constant
     */
    @Override
    public Integer visitNull_const(Null_constContext ctx) {
        stackPush(new Var());
        return 0;
    }

    /**
     * DATE 'YYYY-MM-DD' literal
     */
    @Override
    public Integer visitDate_literal(Date_literalContext ctx) {
        String str = evalPop(ctx.string()).toString();
        stackPush(new Var(Var.Type.DATE, Utils.toDate(str)));
        return 0;
    }

    /**
     * TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFF' literal
     */
    @Override
    public Integer visitTimestamp_literal(Timestamp_literalContext ctx) {
        String str = evalPop(ctx.string()).toString();
        int len = str.length();
        int precision = 0;
        if (len > 19 && len <= 29) {
            precision = len - 20;
            if (precision > 3) {
                precision = 3;
            }
        }
        stackPush(new Var(Utils.toTimestamp(str), precision));
        return 0;
    }

    /**
     * Get the package context within which the current routine is executed
     */
    Package getPackageCallContext() {
        Scope cur = exec.currentScope;
        while (cur != null) {
            if (cur.type == Scope.Type.ROUTINE) {
                return cur.pack;
            }
            cur = cur.parent;
        }
        return null;
    }

    /**
     * Define the connection profile to execute the current statement
     */
    public String getStatementConnection() {
        return exec.conf.defaultConnection;
    }

    /**
     * Define the database type by profile name
     */
    Conn.Type getConnectionType(String conn) {
        return exec.conn.getTypeByProfile(conn);
    }

    /**
     * Get the current database type
     */
    public Conn.Type getConnectionType() {
        return getConnectionType(exec.conf.defaultConnection);
    }

    /**
     * Get node text including spaces
     */
    String getText(ParserRuleContext ctx) {
        return ctx.start.getInputStream()
                .getText(new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    String getText(ParserRuleContext ctx, Token start, Token stop) {
        return ctx.start.getInputStream()
                .getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
    }

    /**
     * Append the text preserving the formatting (space symbols) between tokens
     */
    void append(StringBuilder str, String appendStr, Token start, Token stop) {
        String spaces = start.getInputStream()
                .getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
        spaces = spaces.substring(start.getText().length(), spaces.length() - stop.getText().length());
        str.append(spaces);
        str.append(appendStr);
    }

    void append(StringBuilder str, TerminalNode start, TerminalNode stop) {
        String text = start.getSymbol().getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(start.getSymbol().getStartIndex(),
                        stop.getSymbol().getStopIndex()));
        str.append(text);
    }

    /**
     * Get the first non-null node
     */
    TerminalNode nvl(TerminalNode t1, TerminalNode t2) {
        if (t1 != null) {
            return t1;
        }
        return t2;
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    public Var evalPop(ParserRuleContext ctx) {
        visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    /**
     * Evaluate the data type and length
     */
    String evalPop(DtypeContext type,
            Dtype_lenContext len) {
        if (isConvert(exec.conf.defaultConnection)) {
            return exec.converter.dataType(type, len);
        }
        return getText(type, type.getStart(), len == null ? type.getStop() : len.getStop());
    }

    /**
     * Get formatted text between 2 tokens
     */
    public static String getFormattedText(ParserRuleContext ctx) {
        return ctx.start.getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    /**
     * Flag whether executed from UDF or not
     */
    public void setUdfRun(boolean udfRun) {
        this.udfRun = udfRun;
    }

    /**
     * Whether on-the-fly SQL conversion is required for the connection
     */
    boolean isConvert(String connName) {
        return exec.conf.getConnectionConvert(connName);
    }

    /**
     * Increment the row count
     */
    public int incRowCount() {
        return exec.rowCount++;
    }

    /**
     * Set the row count
     */
    public void setRowCount(int rowCount) {
        exec.rowCount = rowCount;
    }

    /**
     * Trace information
     */
    public void trace(ParserRuleContext ctx, String message) {
        if (!trace) {
            return;
        }
        if (ctx != null) {
            console.printLine("Ln:" + ctx.getStart().getLine() + " " + message);
        } else {
            console.printLine(message);
        }
    }

    /**
     * Trace values retrived from the database
     */
    public void trace(ParserRuleContext ctx, Var var, Metadata meta, int idx) {
        if (var.type != Var.Type.ROW) {
            trace(ctx, "COLUMN: " + meta.columnName(idx) + ", " + meta.columnTypeName(idx));
            trace(ctx, "SET " + var.getName() + " = " + var.toString());
        } else {
            Row row = (Row) var.value;
            int cnt = row.size();
            for (int j = 1; j <= cnt; j++) {
                Var v = row.getValue(j - 1);
                trace(ctx, "COLUMN: " + meta.columnName(j) + ", " + meta.columnTypeName(j));
                trace(ctx, "SET " + v.getName() + " = " + v.toString());
            }
        }
    }

    /**
     * Informational messages
     */
    public void info(ParserRuleContext ctx, String message) {
        if (!info) {
            return;
        }
        if (ctx != null) {
            console.printLine("Ln:" + ctx.getStart().getLine() + " " + message);
        } else {
            console.printLine(message);
        }
    }

    /**
     * Error message
     */
    public void error(ParserRuleContext ctx, String message) {
        if (ctx != null) {
            console.printError("Ln:" + ctx.getStart().getLine() + " " + message);
        } else {
            console.printError(message);
        }
    }

    public Stack<Var> getStack() {
        return exec.stack;
    }

    public int getRowCount() {
        return exec.rowCount;
    }

    public Conf getConf() {
        return exec.conf;
    }

    public Meta getMeta() {
        return exec.meta;
    }

    public boolean getTrace() {
        return exec.trace;
    }

    public boolean getInfo() {
        return exec.info;
    }

    public boolean getOffline() {
        return exec.offline;
    }

    public Console getConsole() {
        return console;
    }

    public void setQueryExecutor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }
}
