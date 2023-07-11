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

package org.apache.doris.hplsql;

import org.apache.doris.hplsql.HplsqlParser.StmtContext;
import org.apache.doris.hplsql.Var.Type;
import org.apache.doris.hplsql.exception.HplValidationException;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.hplsql.exception.TypeException;
import org.apache.doris.hplsql.exception.UndefinedIdentException;
import org.apache.doris.hplsql.executor.JdbcQueryExecutor;
import org.apache.doris.hplsql.executor.Metadata;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.hplsql.executor.ResultListener;
import org.apache.doris.hplsql.functions.BuiltinFunctions;
import org.apache.doris.hplsql.functions.DorisFunctionRegistry;
import org.apache.doris.hplsql.functions.FunctionDatetime;
import org.apache.doris.hplsql.functions.FunctionMisc;
import org.apache.doris.hplsql.functions.FunctionRegistry;
import org.apache.doris.hplsql.functions.FunctionString;
import org.apache.doris.hplsql.functions.InMemoryFunctionRegistry;
import org.apache.doris.hplsql.objects.DbmOutput;
import org.apache.doris.hplsql.objects.DbmOutputClass;
import org.apache.doris.hplsql.objects.HplObject;
import org.apache.doris.hplsql.objects.Method;
import org.apache.doris.hplsql.objects.MethodDictionary;
import org.apache.doris.hplsql.objects.MethodParams;
import org.apache.doris.hplsql.objects.Table;
import org.apache.doris.hplsql.objects.TableClass;
import org.apache.doris.hplsql.objects.UtlFile;
import org.apache.doris.hplsql.objects.UtlFileClass;
import org.apache.doris.hplsql.packages.DorisPackageRegistry;
import org.apache.doris.hplsql.packages.InMemoryPackageRegistry;
import org.apache.doris.hplsql.packages.PackageRegistry;
import org.apache.doris.hplsql.store.MetaClient;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * HPL/SQL script executor
 */
public class Exec extends org.apache.doris.hplsql.HplsqlBaseVisitor<Integer> implements Closeable {

    public static final String VERSION = "HPL/SQL 0.3.31";
    public static final String ERRORCODE = "ERRORCODE";
    public static final String SQLCODE = "SQLCODE";
    public static final String SQLSTATE = "SQLSTATE";
    public static final String HOSTCODE = "HOSTCODE";

    Exec exec;
    FunctionRegistry functions;
    private BuiltinFunctions builtinFunctions;
    private MetaClient client;
    QueryExecutor queryExecutor;
    private PackageRegistry packageRegistry = new InMemoryPackageRegistry();
    private boolean packageLoading = false;
    private Map<String, TableClass> types = new HashMap<>();

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

    public ArrayList<String> stmtConnList = new ArrayList<>();

    Arguments arguments = new Arguments();
    public Conf conf;
    Expression expr;
    Converter converter;
    Meta meta;
    Select select;
    Stmt stmt;
    Conn conn;
    Console console = Console.STANDARD;
    ResultListener resultListener = ResultListener.NONE;

    int rowCount = 0;

    StringBuilder localUdf = new StringBuilder();
    boolean initRoutines = false;
    public boolean buildSql = false;
    public boolean inCallStmt = false;
    boolean udfRegistered = false;
    boolean udfRun = false;

    boolean dotHplsqlrcExists = false;
    boolean hplsqlrcExists = false;

    boolean trace = false;
    boolean info = true;
    boolean offline = false;

    StmtContext lastStmt = null;

    public Exec() {
        exec = this;
        queryExecutor = new JdbcQueryExecutor(this);
    }

    public Exec(Conf conf, Console console, QueryExecutor queryExecutor, ResultListener resultListener) {
        this.conf = conf;
        this.exec = this;
        this.console = console;
        this.queryExecutor = queryExecutor;
        this.resultListener = resultListener;
        this.client = new MetaClient();
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
        if (name.startsWith("hplsql.")) {
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
        String name2 = null;
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
            org.apache.doris.hplsql.HplsqlLexer lexer = new org.apache.doris.hplsql.HplsqlLexer(
                    new ANTLRInputStream(source.get()));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            org.apache.doris.hplsql.HplsqlParser parser = newParser(tokens);
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
        String hplsqlJarName = "hplsql.jar";
        for (String jarName : new java.io.File(dir).list()) {
            if (jarName.startsWith("hive-hplsql") && jarName.endsWith(".jar")) {
                hplsqlJarName = jarName;
                break;
            }
        }
        sql.add("ADD JAR " + dir + hplsqlJarName);
        sql.add("ADD JAR " + dir + "antlr4-runtime-4.5.jar");
        if (!conf.getLocation().equals("")) {
            sql.add("ADD FILE " + conf.getLocation());
        } else {
            sql.add("ADD FILE " + dir + Conf.SITE_XML);
        }
        if (dotHplsqlrcExists) {
            sql.add("ADD FILE " + dir + Conf.DOT_HPLSQLRC);
        }
        if (hplsqlrcExists) {
            sql.add("ADD FILE " + dir + Conf.HPLSQLRC);
        }
        String lu = createLocalUdf();
        if (lu != null) {
            sql.add("ADD FILE " + lu);
        }
        sql.add("CREATE TEMPORARY FUNCTION hplsql AS 'org.apache.doris.udf.hplsql.Udf'");
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
            if (key == null || value == null || !key.startsWith("hplsql.")) {
                continue;
            } else if (key.compareToIgnoreCase(Conf.CONN_DEFAULT) == 0) {
                exec.conf.defaultConnection = value;
            } else if (key.startsWith("hplsql.conn.init.")) {
                exec.conn.addConnectionInit(key.substring(17), value);
            } else if (key.startsWith(Conf.CONN_CONVERT)) {
                exec.conf.setConnectionConvert(key.substring(20), value);
            } else if (key.startsWith("hplsql.conn.")) {
                String name = key.substring(12);
                exec.conn.addConnection(name, value);
            } else if (key.startsWith("hplsql.")) {
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
        select.setResultListener(resultListener);
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
        } finally {
            close();
        }
        return getProgramReturnCode();
    }

    public Var parseAndEval(Arguments arguments) {
        ParseTree tree;
        try (InputStream input = sourceStream(arguments)) {
            tree = parse(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Var result = null;
        try {
            result = evaluate(tree, arguments.main);
        } catch (HplValidationException e) {
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
            exec.functions.exec(execMain.toUpperCase(), null);
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

    private InputStream sourceStream(Arguments arguments) throws FileNotFoundException {
        return arguments.execString != null
                ? new ByteArrayInputStream(arguments.execString.getBytes(StandardCharsets.UTF_8))
                : new FileInputStream(arguments.fileName);
    }

    /**
     * Initialize PL/HQL
     */
    public void init() {
        enterGlobalScope();
        // specify the default log4j2 properties file.
        System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
        if (conf == null) {
            conf = new Conf();
        }
        conf.init();
        conn = new Conn(this);
        meta = new Meta(this, queryExecutor);
        initOptions();

        expr = new Expression(this);
        select = new Select(this, queryExecutor);
        select.setResultListener(resultListener);
        stmt = new Stmt(this, queryExecutor);
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

    private ParseTree parse(InputStream input) throws IOException {
        org.apache.doris.hplsql.HplsqlLexer lexer = new org.apache.doris.hplsql.HplsqlLexer(
                new ANTLRInputStream(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        org.apache.doris.hplsql.HplsqlParser parser = newParser(tokens);
        ParseTree tree = parser.program();
        if (trace) {
            console.printError("Configuration file: " + conf.getLocation());
            console.printError("Parser tree: " + tree.toStringTree(parser));
        }
        return tree;
    }

    protected void registerBuiltins() {
        Var dbmVar = new Var(Type.HPL_OBJECT, "DBMS_OUTPUT");
        DbmOutput dbms = DbmOutputClass.INSTANCE.newInstance();
        dbms.initialize(console);
        dbmVar.setValue(dbms);
        dbmVar.setConstant(true);
        addVariable(dbmVar);

        Var utlFileVar = new Var(Type.HPL_OBJECT, "UTL_FILE");
        UtlFile utlFile = UtlFileClass.INSTANCE.newInstance();
        utlFileVar.setValue(utlFile);
        utlFileVar.setConstant(true);
        addVariable(utlFileVar);
    }

    private org.apache.doris.hplsql.HplsqlParser newParser(CommonTokenStream tokens) {
        org.apache.doris.hplsql.HplsqlParser parser = new org.apache.doris.hplsql.HplsqlParser(tokens);
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
            console.printError(VERSION);
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
     * Include statements from .hplsqlrc and hplsql rc files
     */
    void includeRcFile() {
        if (includeFile(Conf.DOT_HPLSQLRC, false)) {
            dotHplsqlrcExists = true;
        } else {
            if (includeFile(Conf.HPLSQLRC, false)) {
                hplsqlrcExists = true;
            }
        }
        if (udfRun) {
            includeFile(Conf.HPLSQL_LOCALS_SQL, true);
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
        org.apache.doris.hplsql.HplsqlLexer lexer = new org.apache.doris.hplsql.HplsqlLexer(
                new ANTLRInputStream(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        org.apache.doris.hplsql.HplsqlParser parser = newParser(tokens);
        ParseTree tree = parser.program();
        visit(tree);
    }

    /**
     * Start executing HPL/SQL script
     */
    @Override
    public Integer visitProgram(org.apache.doris.hplsql.HplsqlParser.ProgramContext ctx) {
        if (ctx.block() != null) {
            // Record the last stmt. When mysql protocol returns multiple result sets,
            // SERVER_MORE_RESULTS_EXISTS should be specified when sending results other than the last stmt.
            List<StmtContext> stmtContexts = ctx.block().stmt();
            for (int i = stmtContexts.size() - 1; i >= 0; --i) {
                if (stmtContexts.get(i).semicolon_stmt() == null) {
                    lastStmt = stmtContexts.get(i);
                    break;
                }
            }
        }
        return visitChildren(ctx);
    }

    /**
     * Enter BEGIN-END block
     */
    @Override
    public Integer visitBegin_end_block(org.apache.doris.hplsql.HplsqlParser.Begin_end_blockContext ctx) {
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
                error(((HplValidationException) sig.exception).getCtx(), sig.exception.getMessage());
            } else if (sig.type == Signal.Type.SQLEXCEPTION) {
                console.printError("Unhandled exception in HPL/SQL. " + ExceptionUtils.getStackTrace(sig.exception));
            } else if (sig.type == Signal.Type.UNSUPPORTED_OPERATION) {
                console.printError(sig.value == null ? "Unsupported operation" : sig.value);
            } else if (sig.exception != null) {
                console.printError("HPL/SQL error: " + ExceptionUtils.getStackTrace(sig.exception));
            } else if (sig.value != null) {
                console.printError(sig.value);
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
    public Integer visitStmt(org.apache.doris.hplsql.HplsqlParser.StmtContext ctx) {
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
        Integer rc = visitChildren(ctx);
        if (ctx != lastStmt) {
            // printExceptions();
            resultListener.onFinalize();
            console.flushConsole();
        }
        return rc;
    }

    /**
     * Executing or building SELECT statement
     */
    @Override
    public Integer visitSelect_stmt(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx) {
        return exec.select.select(ctx);
    }

    @Override
    public Integer visitCte_select_stmt(org.apache.doris.hplsql.HplsqlParser.Cte_select_stmtContext ctx) {
        return exec.select.cte(ctx);
    }

    @Override
    public Integer visitFullselect_stmt(org.apache.doris.hplsql.HplsqlParser.Fullselect_stmtContext ctx) {
        return exec.select.fullselect(ctx);
    }

    @Override
    public Integer visitSubselect_stmt(org.apache.doris.hplsql.HplsqlParser.Subselect_stmtContext ctx) {
        return exec.select.subselect(ctx);
    }

    @Override
    public Integer visitSelect_list(org.apache.doris.hplsql.HplsqlParser.Select_listContext ctx) {
        return exec.select.selectList(ctx);
    }

    @Override
    public Integer visitFrom_clause(org.apache.doris.hplsql.HplsqlParser.From_clauseContext ctx) {
        return exec.select.from(ctx);
    }

    @Override
    public Integer visitFrom_table_name_clause(org.apache.doris.hplsql.HplsqlParser.From_table_name_clauseContext ctx) {
        return exec.select.fromTable(ctx);
    }

    @Override
    public Integer visitFrom_subselect_clause(org.apache.doris.hplsql.HplsqlParser.From_subselect_clauseContext ctx) {
        return exec.select.fromSubselect(ctx);
    }

    @Override
    public Integer visitFrom_join_clause(org.apache.doris.hplsql.HplsqlParser.From_join_clauseContext ctx) {
        return exec.select.fromJoin(ctx);
    }

    @Override
    public Integer visitFrom_table_values_clause(
            org.apache.doris.hplsql.HplsqlParser.From_table_values_clauseContext ctx) {
        return exec.select.fromTableValues(ctx);
    }

    @Override
    public Integer visitWhere_clause(org.apache.doris.hplsql.HplsqlParser.Where_clauseContext ctx) {
        return exec.select.where(ctx);
    }

    @Override
    public Integer visitSelect_options_item(org.apache.doris.hplsql.HplsqlParser.Select_options_itemContext ctx) {
        return exec.select.option(ctx);
    }

    /**
     * Column name
     */
    @Override
    public Integer visitColumn_name(org.apache.doris.hplsql.HplsqlParser.Column_nameContext ctx) {
        stackPush(meta.normalizeIdentifierPart(ctx.getText()));
        return 0;
    }

    /**
     * Table name
     */
    @Override
    public Integer visitTable_name(org.apache.doris.hplsql.HplsqlParser.Table_nameContext ctx) {
        String name = ctx.getText();
        String nameUp = name.toUpperCase();
        String nameNorm = meta.normalizeObjectIdentifier(name);
        String actualName = exec.managedTables.get(nameUp);
        String conn = exec.objectConnMap.get(nameUp);
        if (conn == null) {
            conn = conf.defaultConnection;
        }
        stmtConnList.add(conn);
        if (actualName != null) {
            stackPush(actualName);
            return 0;
        }
        actualName = exec.objectMap.get(nameUp);
        if (actualName != null) {
            stackPush(actualName);
            return 0;
        }
        stackPush(nameNorm);
        return 0;
    }

    /**
     * SQL INSERT statement
     */
    @Override
    public Integer visitInsert_stmt(org.apache.doris.hplsql.HplsqlParser.Insert_stmtContext ctx) {
        return exec.stmt.insert(ctx);
    }

    /**
     * INSERT DIRECTORY statement
     */
    @Override
    public Integer visitInsert_directory_stmt(org.apache.doris.hplsql.HplsqlParser.Insert_directory_stmtContext ctx) {
        return exec.stmt.insertDirectory(ctx);
    }

    /**
     * EXCEPTION block
     */
    @Override
    public Integer visitException_block_item(org.apache.doris.hplsql.HplsqlParser.Exception_block_itemContext ctx) {
        if (exec.signals.empty()) {
            return 0;
        }
        if (exec.conf.onError == OnError.SETERROR || exec.conf.onError == OnError.STOP) {
            exec.signals.pop();
            return 0;
        }
        if (ctx.L_ID().toString().equalsIgnoreCase("OTHERS")) {
            trace(ctx, "EXCEPTION HANDLER");
            exec.signals.pop();
            enterScope(Scope.Type.HANDLER);
            visit(ctx.block());
            leaveScope();
        }
        return 0;
    }

    /**
     * DECLARE variable statement
     */
    @Override
    public Integer visitDeclare_var_item(org.apache.doris.hplsql.HplsqlParser.Declare_var_itemContext ctx) {
        String type = null;
        TableClass userDefinedType = null;
        Row row = null;
        String len = null;
        String scale = null;
        Var defaultVar = null;
        if (ctx.dtype().T_ROWTYPE() != null) {
            row = meta.getRowDataType(ctx, exec.conf.defaultConnection, ctx.dtype().qident().getText());
            if (row == null) {
                type = Var.DERIVED_ROWTYPE;
            }
        } else {
            type = getDataType(ctx);
            if (ctx.dtype_len() != null) {
                len = ctx.dtype_len().L_INT(0).getText();
                if (ctx.dtype_len().L_INT(1) != null) {
                    scale = ctx.dtype_len().L_INT(1).getText();
                }
            }
            if (ctx.dtype_default() != null) {
                defaultVar = evalPop(ctx.dtype_default());
            }
            userDefinedType = types.get(type);
            if (userDefinedType != null) {
                type = Type.HPL_OBJECT.name();
            }

        }
        int cnt = ctx.ident().size();        // Number of variables declared with the same data type and default
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident(i).getText();
            if (row == null) {
                Var var = new Var(name, type, len, scale, defaultVar);
                if (userDefinedType != null && defaultVar == null) {
                    var.setValue(userDefinedType.newInstance());
                }
                exec.addVariable(var);
                if (ctx.T_CONSTANT() != null) {
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
    String getDataType(org.apache.doris.hplsql.HplsqlParser.Declare_var_itemContext ctx) {
        String type;
        if (ctx.dtype().T_TYPE() != null) {
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
    public Integer visitAllocate_cursor_stmt(org.apache.doris.hplsql.HplsqlParser.Allocate_cursor_stmtContext ctx) {
        return exec.stmt.allocateCursor(ctx);
    }

    /**
     * ASSOCIATE LOCATOR statement
     */
    @Override
    public Integer visitAssociate_locator_stmt(org.apache.doris.hplsql.HplsqlParser.Associate_locator_stmtContext ctx) {
        return exec.stmt.associateLocator(ctx);
    }

    /**
     * DECLARE cursor statement
     */
    @Override
    public Integer visitDeclare_cursor_item(org.apache.doris.hplsql.HplsqlParser.Declare_cursor_itemContext ctx) {
        return exec.stmt.declareCursor(ctx);
    }

    /**
     * DESCRIBE statement
     */
    @Override
    public Integer visitDescribe_stmt(org.apache.doris.hplsql.HplsqlParser.Describe_stmtContext ctx) {
        return exec.stmt.describe(ctx);
    }

    /**
     * DROP statement
     */
    @Override
    public Integer visitDrop_stmt(org.apache.doris.hplsql.HplsqlParser.Drop_stmtContext ctx) {
        return exec.stmt.drop(ctx);
    }

    /**
     * OPEN cursor statement
     */
    @Override
    public Integer visitOpen_stmt(org.apache.doris.hplsql.HplsqlParser.Open_stmtContext ctx) {
        return exec.stmt.open(ctx);
    }

    /**
     * FETCH cursor statement
     */
    @Override
    public Integer visitFetch_stmt(org.apache.doris.hplsql.HplsqlParser.Fetch_stmtContext ctx) {
        return exec.stmt.fetch(ctx);
    }

    /**
     * CLOSE cursor statement
     */
    @Override
    public Integer visitClose_stmt(org.apache.doris.hplsql.HplsqlParser.Close_stmtContext ctx) {
        return exec.stmt.close(ctx);
    }

    /**
     * CMP statement
     */
    @Override
    public Integer visitCmp_stmt(org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext ctx) {
        return new Cmp(exec, queryExecutor).run(ctx);
    }

    /**
     * COPY statement
     */
    @Override
    public Integer visitCopy_stmt(org.apache.doris.hplsql.HplsqlParser.Copy_stmtContext ctx) {
        return new Copy(exec, queryExecutor).run(ctx);
    }

    /**
     * COPY FROM LOCAL statement
     */
    @Override
    public Integer visitCopy_from_local_stmt(org.apache.doris.hplsql.HplsqlParser.Copy_from_local_stmtContext ctx) {
        return new Copy(exec, queryExecutor).runFromLocal(ctx);
    }

    /**
     * DECLARE HANDLER statement
     */
    @Override
    public Integer visitDeclare_handler_item(org.apache.doris.hplsql.HplsqlParser.Declare_handler_itemContext ctx) {
        trace(ctx, "DECLARE HANDLER");
        Handler.ExecType execType = Handler.ExecType.EXIT;
        Signal.Type type = Signal.Type.SQLEXCEPTION;
        String value = null;
        if (ctx.T_CONTINUE() != null) {
            execType = Handler.ExecType.CONTINUE;
        }
        if (ctx.ident() != null) {
            type = Signal.Type.USERDEFINED;
            value = ctx.ident().getText();
        } else if (ctx.T_NOT() != null && ctx.T_FOUND() != null) {
            type = Signal.Type.NOTFOUND;
        }
        addHandler(new Handler(execType, type, value, exec.currentScope, ctx));
        return 0;
    }

    /**
     * DECLARE CONDITION
     */
    @Override
    public Integer visitDeclare_condition_item(org.apache.doris.hplsql.HplsqlParser.Declare_condition_itemContext ctx) {
        return 0;
    }

    /**
     * DECLARE TEMPORARY TABLE statement
     */
    @Override
    public Integer visitDeclare_temporary_table_item(
            org.apache.doris.hplsql.HplsqlParser.Declare_temporary_table_itemContext ctx) {
        return exec.stmt.declareTemporaryTable(ctx);
    }

    /**
     * CREATE TABLE statement
     */
    @Override
    public Integer visitCreate_table_stmt(org.apache.doris.hplsql.HplsqlParser.Create_table_stmtContext ctx) {
        return exec.stmt.createTable(ctx);
    }

    @Override
    public Integer visitCreate_table_options_hive_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_hive_itemContext ctx) {
        return exec.stmt.createTableHiveOptions(ctx);
    }

    @Override
    public Integer visitCreate_table_options_ora_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_ora_itemContext ctx) {
        return 0;
    }

    @Override
    public Integer visitCreate_table_options_td_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_td_itemContext ctx) {
        return 0;
    }

    @Override
    public Integer visitCreate_table_options_mssql_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_mssql_itemContext ctx) {
        return 0;
    }

    @Override
    public Integer visitCreate_table_options_db2_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_db2_itemContext ctx) {
        return 0;
    }

    @Override
    public Integer visitCreate_table_options_mysql_item(
            org.apache.doris.hplsql.HplsqlParser.Create_table_options_mysql_itemContext ctx) {
        return exec.stmt.createTableMysqlOptions(ctx);
    }

    /**
     * CREATE LOCAL TEMPORARY | VOLATILE TABLE statement
     */
    @Override
    public Integer visitCreate_local_temp_table_stmt(
            org.apache.doris.hplsql.HplsqlParser.Create_local_temp_table_stmtContext ctx) {
        return exec.stmt.createLocalTemporaryTable(ctx);
    }

    /**
     * ALTER TABLE statement
     */
    @Override
    public Integer visitAlter_table_stmt(org.apache.doris.hplsql.HplsqlParser.Alter_table_stmtContext ctx) {
        return 0;
    }

    /**
     * CREATE DATABASE | SCHEMA statement
     */
    @Override
    public Integer visitCreate_database_stmt(org.apache.doris.hplsql.HplsqlParser.Create_database_stmtContext ctx) {
        return exec.stmt.createDatabase(ctx);
    }

    /**
     * CREATE FUNCTION statement
     */
    @Override
    public Integer visitCreate_function_stmt(org.apache.doris.hplsql.HplsqlParser.Create_function_stmtContext ctx) {
        exec.functions.addUserFunction(ctx);
        addLocalUdf(ctx);
        return 0;
    }

    /**
     * CREATE PACKAGE specification statement
     */
    @Override
    public Integer visitCreate_package_stmt(org.apache.doris.hplsql.HplsqlParser.Create_package_stmtContext ctx) {
        String name = ctx.ident(0).getText().toUpperCase();
        if (exec.packageLoading) {
            exec.currentPackageDecl = new Package(name, exec, builtinFunctions);
            exec.packages.put(name, exec.currentPackageDecl);
            exec.currentPackageDecl.createSpecification(ctx);
            exec.currentPackageDecl = null;
        } else {
            trace(ctx, "CREATE PACKAGE");
            exec.packages.remove(name);
            exec.packageRegistry.createPackageHeader(name, getFormattedText(ctx), ctx.T_REPLACE() != null);
        }
        return 0;
    }

    /**
     * CREATE PACKAGE body statement
     */
    @Override
    public Integer visitCreate_package_body_stmt(
            org.apache.doris.hplsql.HplsqlParser.Create_package_body_stmtContext ctx) {
        String name = ctx.ident(0).getText().toUpperCase();
        if (exec.packageLoading) {
            exec.currentPackageDecl = exec.packages.get(name);
            if (exec.currentPackageDecl == null) {
                exec.currentPackageDecl = new Package(name, exec, builtinFunctions);
                exec.currentPackageDecl.setAllMembersPublic(true);
                exec.packages.put(name, exec.currentPackageDecl);
            }
            exec.currentPackageDecl.createBody(ctx);
            exec.currentPackageDecl = null;
        } else {
            trace(ctx, "CREATE PACKAGE BODY");
            exec.packages.remove(name);
            exec.packageRegistry.createPackageBody(name, getFormattedText(ctx), ctx.T_REPLACE() != null);
        }
        return 0;
    }

    /**
     * CREATE PROCEDURE statement
     */
    @Override
    public Integer visitCreate_procedure_stmt(org.apache.doris.hplsql.HplsqlParser.Create_procedure_stmtContext ctx) {
        exec.functions.addUserProcedure(ctx);
        addLocalUdf(ctx);                      // Add procedures as they can be invoked by functions
        return 0;
    }

    public void dropProcedure(org.apache.doris.hplsql.HplsqlParser.Drop_stmtContext ctx, String name,
            boolean checkIfExists) {
        if (checkIfExists && !functions.exists(name)) {
            trace(ctx, name + " DOES NOT EXIST");
            return;
        }
        functions.remove(name);
        trace(ctx, name + " DROPPED");
    }

    public void dropPackage(org.apache.doris.hplsql.HplsqlParser.Drop_stmtContext ctx, String name,
            boolean checkIfExists) {
        if (checkIfExists && !packageRegistry.getPackage(name).isPresent()) {
            trace(ctx, name + " DOES NOT EXIST");
            return;
        }
        packages.remove(name);
        packageRegistry.dropPackage(name);
        trace(ctx, name + " DROPPED");
    }

    /**
     * CREATE INDEX statement
     */
    @Override
    public Integer visitCreate_index_stmt(org.apache.doris.hplsql.HplsqlParser.Create_index_stmtContext ctx) {
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
            String file = System.getProperty("user.dir") + "/" + Conf.HPLSQL_LOCALS_SQL;
            PrintWriter writer = new PrintWriter(file, "UTF-8");
            writer.print(localUdf);
            writer.close();
            return file;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Integer visitSet_doris_session_option(
            org.apache.doris.hplsql.HplsqlParser.Set_doris_session_optionContext ctx) {
        StringBuilder sql = new StringBuilder("set ");
        for (int i = 0; i < ctx.getChildCount(); i++) {
            sql.append(ctx.getChild(i).getText()).append(" ");
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
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
            org.apache.doris.hplsql.HplsqlParser.Assignment_stmt_single_itemContext ctx) {
        String name = ctx.ident().getText();
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
            org.apache.doris.hplsql.HplsqlParser.Assignment_stmt_multiple_itemContext ctx) {
        int cnt = ctx.ident().size();
        int ecnt = ctx.expr().size();
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident(i).getText();
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
            org.apache.doris.hplsql.HplsqlParser.Assignment_stmt_select_itemContext ctx) {
        return stmt.assignFromSelect(ctx);
    }

    @Override
    public Integer visitAssignment_stmt_collection_item(
            org.apache.doris.hplsql.HplsqlParser.Assignment_stmt_collection_itemContext ctx) {
        org.apache.doris.hplsql.HplsqlParser.Expr_funcContext lhs = ctx.expr_func();
        Var var = findVariable(lhs.ident().getText());
        if (var == null || var.type != Type.HPL_OBJECT) {
            stackPush(Var.Null);
            return 0;
        }
        MethodParams.Arity.UNARY.check(lhs.ident().getText(), lhs.expr_func_params().func_param());
        Var index = evalPop(lhs.expr_func_params().func_param(0));
        Var value = evalPop(ctx.expr());
        dispatch(ctx, (HplObject) var.value, MethodDictionary.__SETITEM__, Arrays.asList(index, value));
        return 0;
    }

    /**
     * Evaluate an expression
     */
    @Override
    public Integer visitExpr(org.apache.doris.hplsql.HplsqlParser.ExprContext ctx) {
        if (exec.buildSql) {
            exec.expr.execSql(ctx);
        } else {
            exec.expr.exec(ctx);
        }
        return 0;
    }

    /**
     * Evaluate a boolean expression
     */
    @Override
    public Integer visitBool_expr(org.apache.doris.hplsql.HplsqlParser.Bool_exprContext ctx) {
        if (exec.buildSql) {
            exec.expr.execBoolSql(ctx);
        } else {
            exec.expr.execBool(ctx);
        }
        return 0;
    }

    @Override
    public Integer visitBool_expr_binary(org.apache.doris.hplsql.HplsqlParser.Bool_expr_binaryContext ctx) {
        if (exec.buildSql) {
            exec.expr.execBoolBinarySql(ctx);
        } else {
            exec.expr.execBoolBinary(ctx);
        }
        return 0;
    }

    @Override
    public Integer visitBool_expr_unary(org.apache.doris.hplsql.HplsqlParser.Bool_expr_unaryContext ctx) {
        if (exec.buildSql) {
            exec.expr.execBoolUnarySql(ctx);
        } else {
            exec.expr.execBoolUnary(ctx);
        }
        return 0;
    }

    /**
     * Static SELECT statement (i.e. unquoted) or expression
     */
    @Override
    public Integer visitExpr_select(org.apache.doris.hplsql.HplsqlParser.Expr_selectContext ctx) {
        if (ctx.select_stmt() != null) {
            stackPush(new Var(evalPop(ctx.select_stmt())));
        } else {
            visit(ctx.expr());
        }
        return 0;
    }

    /**
     * File path (unquoted) or expression
     */
    @Override
    public Integer visitExpr_file(org.apache.doris.hplsql.HplsqlParser.Expr_fileContext ctx) {
        if (ctx.file_name() != null) {
            stackPush(new Var(ctx.file_name().getText()));
        } else {
            visit(ctx.expr());
        }
        return 0;
    }

    /**
     * Cursor attribute %ISOPEN, %FOUND and %NOTFOUND
     */
    @Override
    public Integer visitExpr_cursor_attribute(org.apache.doris.hplsql.HplsqlParser.Expr_cursor_attributeContext ctx) {
        exec.expr.execCursorAttribute(ctx);
        return 0;
    }

    /**
     * Function call
     */
    @Override
    public Integer visitExpr_func(org.apache.doris.hplsql.HplsqlParser.Expr_funcContext ctx) {
        return functionCall(ctx, ctx.ident(), ctx.expr_func_params());
    }

    private int functionCall(ParserRuleContext ctx, org.apache.doris.hplsql.HplsqlParser.IdentContext ident,
            org.apache.doris.hplsql.HplsqlParser.Expr_func_paramsContext params) {
        String name = ident.getText();
        if (exec.buildSql) {
            exec.execSql(name, params);
        } else {
            name = name.toUpperCase();
            Package packCallContext = exec.getPackageCallContext();
            ArrayList<String> qualified = exec.meta.splitIdentifier(name);
            boolean executed = false;
            if (qualified != null) {
                Package pack = findPackage(qualified.get(0));
                if (pack != null) {
                    executed = pack.execFunc(qualified.get(1), params);
                }
            }
            if (!executed && packCallContext != null) {
                executed = packCallContext.execFunc(name, params);
            }
            if (!executed) {
                if (!exec.functions.exec(name, params)) {
                    Var var = findVariable(name);
                    if (var != null && var.type == Type.HPL_OBJECT) {
                        stackPush(dispatch(ctx, (HplObject) var.value, MethodDictionary.__GETITEM__, params));
                    } else {
                        throw new UndefinedIdentException(ctx, name);
                    }
                }
            }
        }
        return 0;
    }

    private Var dispatch(ParserRuleContext ctx, HplObject obj, String methodName,
            org.apache.doris.hplsql.HplsqlParser.Expr_func_paramsContext paramCtx) {
        List<Var> params = paramCtx == null
                ? Collections.emptyList()
                : paramCtx.func_param().stream().map(this::evalPop).collect(Collectors.toList());
        return dispatch(ctx, obj, methodName, params);
    }

    private Var dispatch(ParserRuleContext ctx, HplObject obj, String methodName, List<Var> params) {
        Method method = obj.hplClass().methodDictionary().get(ctx, methodName);
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
            } else if (var.type == Type.HPL_OBJECT && var.value instanceof Table) {
                tables.add((Table) var.value);
            } else {
                throw new TypeException(ctx, Table.class, var.type, var.value);
            }
        }
        if (tables.size() > 1 && tables.stream().anyMatch(tbl -> tbl.hplClass().rowType())) {
            throw new TypeException(ctx, "rowtype table should not be used when selecting into multiple tables");
        }
        return tables;
    }

    /**
     * User-defined function in a SQL query
     */
    public void execSql(String name, org.apache.doris.hplsql.HplsqlParser.Expr_func_paramsContext ctx) {
        if (execUserSql(ctx, name)) {
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append(name);
        sql.append("(");
        if (ctx != null) {
            int cnt = ctx.func_param().size();
            for (int i = 0; i < cnt; i++) {
                sql.append(evalPop(ctx.func_param(i).expr()));
                if (i + 1 < cnt) {
                    sql.append(", ");
                }
            }
        }
        sql.append(")");
        exec.stackPush(sql);
    }

    /**
     * Execute a HPL/SQL user-defined function in a query.
     * For example converts: select fn(col) from table to select hplsql('fn(:1)', col) from table
     */
    private boolean execUserSql(org.apache.doris.hplsql.HplsqlParser.Expr_func_paramsContext ctx, String name) {
        if (!functions.exists(name)) {
            return false;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("hplsql('");
        sql.append(name);
        sql.append("(");
        int cnt = ctx.func_param().size();
        for (int i = 0; i < cnt; i++) {
            sql.append(":").append(i + 1);
            if (i + 1 < cnt) {
                sql.append(", ");
            }
        }
        sql.append(")'");
        if (cnt > 0) {
            sql.append(", ");
        }
        for (int i = 0; i < cnt; i++) {
            sql.append(evalPop(ctx.func_param(i).expr()));
            if (i + 1 < cnt) {
                sql.append(", ");
            }
        }
        sql.append(")");
        exec.stackPush(sql);
        exec.registerUdf();
        return true;
    }

    /**
     * Aggregate or window function call
     */
    @Override
    public Integer visitExpr_agg_window_func(org.apache.doris.hplsql.HplsqlParser.Expr_agg_window_funcContext ctx) {
        exec.stackPush(Exec.getFormattedText(ctx));
        return 0;
    }

    /**
     * Function with specific syntax
     */
    @Override
    public Integer visitExpr_spec_func(org.apache.doris.hplsql.HplsqlParser.Expr_spec_funcContext ctx) {
        if (exec.buildSql) {
            exec.builtinFunctions.specExecSql(ctx);
        } else {
            exec.builtinFunctions.specExec(ctx);
        }
        return 0;
    }

    /**
     * INCLUDE statement
     */
    @Override
    public Integer visitInclude_stmt(@NotNull org.apache.doris.hplsql.HplsqlParser.Include_stmtContext ctx) {
        return exec.stmt.include(ctx);
    }

    /**
     * IF statement (PL/SQL syntax)
     */
    @Override
    public Integer visitIf_plsql_stmt(org.apache.doris.hplsql.HplsqlParser.If_plsql_stmtContext ctx) {
        return exec.stmt.ifPlsql(ctx);
    }

    /**
     * IF statement (Transact-SQL syntax)
     */
    @Override
    public Integer visitIf_tsql_stmt(org.apache.doris.hplsql.HplsqlParser.If_tsql_stmtContext ctx) {
        return exec.stmt.ifTsql(ctx);
    }

    /**
     * IF statement (BTEQ syntax)
     */
    @Override
    public Integer visitIf_bteq_stmt(org.apache.doris.hplsql.HplsqlParser.If_bteq_stmtContext ctx) {
        return exec.stmt.ifBteq(ctx);
    }

    /**
     * USE statement
     */
    @Override
    public Integer visitUse_stmt(org.apache.doris.hplsql.HplsqlParser.Use_stmtContext ctx) {
        return exec.stmt.use(ctx);
    }

    /**
     * VALUES statement
     */
    @Override
    public Integer visitValues_into_stmt(org.apache.doris.hplsql.HplsqlParser.Values_into_stmtContext ctx) {
        return exec.stmt.values(ctx);
    }

    /**
     * WHILE statement
     */
    @Override
    public Integer visitWhile_stmt(org.apache.doris.hplsql.HplsqlParser.While_stmtContext ctx) {
        return exec.stmt.while_(ctx);
    }

    @Override
    public Integer visitUnconditional_loop_stmt(
            org.apache.doris.hplsql.HplsqlParser.Unconditional_loop_stmtContext ctx) {
        return exec.stmt.unconditionalLoop(ctx);
    }

    /**
     * FOR cursor statement
     */
    @Override
    public Integer visitFor_cursor_stmt(org.apache.doris.hplsql.HplsqlParser.For_cursor_stmtContext ctx) {
        return exec.stmt.forCursor(ctx);
    }

    /**
     * FOR (integer range) statement
     */
    @Override
    public Integer visitFor_range_stmt(org.apache.doris.hplsql.HplsqlParser.For_range_stmtContext ctx) {
        return exec.stmt.forRange(ctx);
    }

    /**
     * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL
     */
    @Override
    public Integer visitExec_stmt(org.apache.doris.hplsql.HplsqlParser.Exec_stmtContext ctx) {
        exec.inCallStmt = true;
        Integer rc = exec.stmt.exec(ctx);
        exec.inCallStmt = false;
        return rc;
    }

    /**
     * CALL statement
     */
    @Override
    public Integer visitCall_stmt(org.apache.doris.hplsql.HplsqlParser.Call_stmtContext ctx) {
        exec.inCallStmt = true;
        try {
            if (ctx.expr_func() != null) {
                functionCall(ctx, ctx.expr_func().ident(), ctx.expr_func().expr_func_params());
            } else if (ctx.expr_dot() != null) {
                visitExpr_dot(ctx.expr_dot());
            } else if (ctx.ident() != null) {
                functionCall(ctx, ctx.ident(), null);
            }
        } finally {
            exec.inCallStmt = false;
        }
        return 0;
    }

    /**
     * EXIT statement (leave the specified loop with a condition)
     */
    @Override
    public Integer visitExit_stmt(org.apache.doris.hplsql.HplsqlParser.Exit_stmtContext ctx) {
        return exec.stmt.exit(ctx);
    }

    /**
     * BREAK statement (leave the innermost loop unconditionally)
     */
    @Override
    public Integer visitBreak_stmt(org.apache.doris.hplsql.HplsqlParser.Break_stmtContext ctx) {
        return exec.stmt.break_(ctx);
    }

    /**
     * LEAVE statement (leave the specified loop unconditionally)
     */
    @Override
    public Integer visitLeave_stmt(org.apache.doris.hplsql.HplsqlParser.Leave_stmtContext ctx) {
        return exec.stmt.leave(ctx);
    }

    /**
     * PRINT statement
     */
    @Override
    public Integer visitPrint_stmt(org.apache.doris.hplsql.HplsqlParser.Print_stmtContext ctx) {
        return exec.stmt.print(ctx);
    }

    /**
     * QUIT statement
     */
    @Override
    public Integer visitQuit_stmt(org.apache.doris.hplsql.HplsqlParser.Quit_stmtContext ctx) {
        return exec.stmt.quit(ctx);
    }

    /**
     * SIGNAL statement
     */
    @Override
    public Integer visitSignal_stmt(org.apache.doris.hplsql.HplsqlParser.Signal_stmtContext ctx) {
        return exec.stmt.signal(ctx);
    }

    /**
     * SUMMARY statement
     */
    @Override
    public Integer visitSummary_stmt(org.apache.doris.hplsql.HplsqlParser.Summary_stmtContext ctx) {
        return exec.stmt.summary(ctx);
    }

    /**
     * RESIGNAL statement
     */
    @Override
    public Integer visitResignal_stmt(org.apache.doris.hplsql.HplsqlParser.Resignal_stmtContext ctx) {
        return exec.stmt.resignal(ctx);
    }

    /**
     * RETURN statement
     */
    @Override
    public Integer visitReturn_stmt(org.apache.doris.hplsql.HplsqlParser.Return_stmtContext ctx) {
        return exec.stmt.return_(ctx);
    }

    /**
     * SET session options
     */
    @Override
    public Integer visitSet_current_schema_option(
            org.apache.doris.hplsql.HplsqlParser.Set_current_schema_optionContext ctx) {
        return exec.stmt.setCurrentSchema(ctx);
    }

    /**
     * TRUNCATE statement
     */
    @Override
    public Integer visitTruncate_stmt(org.apache.doris.hplsql.HplsqlParser.Truncate_stmtContext ctx) {
        return exec.stmt.truncate(ctx);
    }

    @Override
    public Integer visitCreate_table_type_stmt(org.apache.doris.hplsql.HplsqlParser.Create_table_type_stmtContext ctx) {
        String name = ctx.ident().getText();
        String index = ctx.dtype().getText();
        if (!"BINARY_INTEGER".equalsIgnoreCase(index)) {
            throw new TypeException(ctx, "Unsupported table index: " + index + " Use: BINARY_INTEGER");
        }
        org.apache.doris.hplsql.HplsqlParser.Tbl_typeContext tblType = ctx.tbl_type();
        if (tblType.sql_type() != null) {
            String dbTable = tblType.sql_type().qident().getText();
            if (tblType.sql_type().T_ROWTYPE() != null) {
                Row rowType = meta.getRowDataType(ctx, exec.conf.defaultConnection, dbTable);
                exec.addType(new TableClass(name, rowType.columnDefinitions(), true));
            } else if (dbTable.contains(".")) { // column type
                String column = dbTable.substring(dbTable.indexOf(".") + 1);
                String colType = meta.getDataType(ctx, exec.conf.defaultConnection, dbTable);
                exec.addType(new TableClass(name,
                        Collections.singletonList(new ColumnDefinition(column, ColumnType.parse(colType))), false));
            } else {
                throw new TypeException(ctx, "Invalid table type attribute. Expected %TYPE or %ROWTYPE");
            }
            if (trace) {
                trace(ctx, "CREATE TABLE TYPE: " + name + " TYPE: " + dbTable + " INDEX: " + index);
            }
        } else {
            String colType = tblType.dtype().getText();
            exec.addType(
                    new TableClass(name, Collections.singletonList(ColumnDefinition.unnamed(ColumnType.parse(colType))),
                            false));
            if (trace) {
                trace(ctx, "CREATE TABLE TYPE: " + name + " TYPE: " + colType + " INDEX: " + index);
            }
        }
        return 1;
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
    public Integer visitMap_object_stmt(org.apache.doris.hplsql.HplsqlParser.Map_object_stmtContext ctx) {
        String source = ctx.ident(0).getText();
        String target = null;
        String conn = null;
        if (ctx.T_TO() != null) {
            target = ctx.ident(1).getText();
            exec.objectMap.put(source.toUpperCase(), target);
        }
        if (ctx.T_AT() != null) {
            if (ctx.T_TO() == null) {
                conn = ctx.ident(1).getText();
            } else {
                conn = ctx.ident(2).getText();
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
     * UPDATE statement
     */
    @Override
    public Integer visitUpdate_stmt(org.apache.doris.hplsql.HplsqlParser.Update_stmtContext ctx) {
        return stmt.update(ctx);
    }

    /**
     * DELETE statement
     */
    @Override
    public Integer visitDelete_stmt(org.apache.doris.hplsql.HplsqlParser.Delete_stmtContext ctx) {
        return stmt.delete(ctx);
    }

    /**
     * MERGE statement
     */
    @Override
    public Integer visitMerge_stmt(org.apache.doris.hplsql.HplsqlParser.Merge_stmtContext ctx) {
        return stmt.merge(ctx);
    }

    /**
     * Run a Hive command line
     */
    @Override
    public Integer visitHive(@NotNull org.apache.doris.hplsql.HplsqlParser.HiveContext ctx) {
        trace(ctx, "HIVE");
        ArrayList<String> cmd = new ArrayList<>();
        cmd.add("hive");
        Var params = new Var(Var.Type.STRINGLIST, cmd);
        stackPush(params);
        visitChildren(ctx);
        stackPop();
        try {
            String[] cmdarr = new String[cmd.size()];
            cmd.toArray(cmdarr);
            if (trace) {
                trace(ctx, "HIVE Parameters: " + Utils.toString(cmdarr, ' '));
            }
            if (!offline) {
                Process p = Runtime.getRuntime().exec(cmdarr);
                new StreamGobbler(p.getInputStream(), console).start();
                new StreamGobbler(p.getErrorStream(), console).start();
                int rc = p.waitFor();
                if (trace) {
                    trace(ctx, "HIVE Process exit code: " + rc);
                }
            }
        } catch (Exception e) {
            setSqlCode(SqlCodes.ERROR);
            signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
            return -1;
        }
        return 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Integer visitHive_item(org.apache.doris.hplsql.HplsqlParser.Hive_itemContext ctx) {
        Var params = stackPeek();
        ArrayList<String> a = (ArrayList<String>) params.value;
        String param = ctx.getChild(1).getText();
        switch (param) {
            case "e":
                a.add("-e");
                a.add(evalPop(ctx.expr()).toString());
                break;
            case "f":
                a.add("-f");
                a.add(evalPop(ctx.expr()).toString());
                break;
            case "hiveconf":
                a.add("-hiveconf");
                a.add(ctx.L_ID().toString() + "=" + evalPop(ctx.expr()).toString());
                break;
            default:
        }
        return 0;
    }

    /**
     * Executing OS command
     */
    @Override
    public Integer visitHost_cmd(org.apache.doris.hplsql.HplsqlParser.Host_cmdContext ctx) {
        trace(ctx, "HOST");
        execHost(ctx, ctx.start.getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex())));
        return 0;
    }

    @Override
    public Integer visitHost_stmt(org.apache.doris.hplsql.HplsqlParser.Host_stmtContext ctx) {
        trace(ctx, "HOST");
        execHost(ctx, evalPop(ctx.expr()).toString());
        return 0;
    }

    public void execHost(ParserRuleContext ctx, String cmd) {
        try {
            if (trace) {
                trace(ctx, "HOST Command: " + cmd);
            }
            Process p = Runtime.getRuntime().exec(cmd);
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
        }
    }

    /**
     * Standalone expression (as a statement)
     */
    @Override
    public Integer visitExpr_stmt(org.apache.doris.hplsql.HplsqlParser.Expr_stmtContext ctx) {
        visitChildren(ctx);
        return 0;
    }

    /**
     * String concatenation operator
     */
    @Override
    public Integer visitExpr_concat(org.apache.doris.hplsql.HplsqlParser.Expr_concatContext ctx) {
        if (exec.buildSql) {
            exec.expr.operatorConcatSql(ctx);
        } else {
            exec.expr.operatorConcat(ctx);
        }
        return 0;
    }

    @Override
    public Integer visitExpr_dot_method_call(org.apache.doris.hplsql.HplsqlParser.Expr_dot_method_callContext ctx) {
        if (exec.buildSql) {
            exec.stackPush(new Var(Var.Type.IDENT, ctx.getText()));
            return 0;
        }
        Var var = ctx.ident() != null
                ? findVariable(ctx.ident().getText())
                : evalPop(ctx.expr_func(0));

        if (var == null && ctx.ident() != null) {
            Package pkg = findPackage(ctx.ident().getText());
            String pkgFuncName = ctx.expr_func(0).ident().getText().toUpperCase();
            boolean executed = pkg.execFunc(pkgFuncName, ctx.expr_func(0).expr_func_params());
            Package packCallContext = exec.getPackageCallContext();
            if (!executed && packCallContext != null) {
                packCallContext.execFunc(pkgFuncName, ctx.expr_func(0).expr_func_params());
            }
            return 0;
        }

        org.apache.doris.hplsql.HplsqlParser.Expr_funcContext method = ctx.expr_func(ctx.expr_func().size() - 1);
        switch (var.type) {
            case HPL_OBJECT:
                Var result = dispatch(ctx, (HplObject) var.value, method.ident().getText(), method.expr_func_params());
                stackPush(result);
                return 0;
            default:
                throw new TypeException(ctx, var.type + " is not an object");
        }
    }

    @Override
    public Integer visitExpr_dot_property_access(
            org.apache.doris.hplsql.HplsqlParser.Expr_dot_property_accessContext ctx) {
        if (exec.buildSql) {
            exec.stackPush(new Var(Var.Type.IDENT, ctx.getText()));
            return 0;
        }
        Var var = ctx.expr_func() != null
                ? evalPop(ctx.expr_func())
                : findVariable(ctx.ident(0).getText());
        String property = ctx.ident(ctx.ident().size() - 1).getText();

        if (var == null && ctx.expr_func() == null) {
            Package pkg = findPackage(ctx.ident(0).getText());
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
            case HPL_OBJECT:
                Var result = dispatch(ctx, (HplObject) var.value, property, Collections.emptyList());
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
    public Integer visitExpr_case_simple(org.apache.doris.hplsql.HplsqlParser.Expr_case_simpleContext ctx) {
        if (exec.buildSql) {
            exec.expr.execSimpleCaseSql(ctx);
        } else {
            exec.expr.execSimpleCase(ctx);
        }
        return 0;
    }

    /**
     * Searched CASE expression
     */
    @Override
    public Integer visitExpr_case_searched(org.apache.doris.hplsql.HplsqlParser.Expr_case_searchedContext ctx) {
        if (exec.buildSql) {
            exec.expr.execSearchedCaseSql(ctx);
        } else {
            exec.expr.execSearchedCase(ctx);
        }
        return 0;
    }

    /**
     * GET DIAGNOSTICS EXCEPTION statement
     */
    @Override
    public Integer visitGet_diag_stmt_exception_item(
            org.apache.doris.hplsql.HplsqlParser.Get_diag_stmt_exception_itemContext ctx) {
        return exec.stmt.getDiagnosticsException(ctx);
    }

    /**
     * GET DIAGNOSTICS ROW_COUNT statement
     */
    @Override
    public Integer visitGet_diag_stmt_rowcount_item(
            org.apache.doris.hplsql.HplsqlParser.Get_diag_stmt_rowcount_itemContext ctx) {
        return exec.stmt.getDiagnosticsRowCount(ctx);
    }

    /**
     * GRANT statement
     */
    @Override
    public Integer visitGrant_stmt(org.apache.doris.hplsql.HplsqlParser.Grant_stmtContext ctx) {
        trace(ctx, "GRANT");
        return 0;
    }

    /**
     * Label
     */
    @Override
    public Integer visitLabel(org.apache.doris.hplsql.HplsqlParser.LabelContext ctx) {
        if (ctx.L_ID() != null) {
            exec.labels.push(ctx.L_ID().toString());
        } else {
            String label = ctx.L_LABEL().getText();
            if (label.endsWith(":")) {
                label = label.substring(0, label.length() - 1);
            }
            exec.labels.push(label);
        }
        return 0;
    }

    /**
     * Identifier
     */
    @Override
    public Integer visitIdent(org.apache.doris.hplsql.HplsqlParser.IdentContext ctx) {
        boolean hasSub = false;
        String ident = ctx.getText();
        String actualIdent = ident;
        if (ident.startsWith("-")) {
            hasSub = true;
            actualIdent = ident.substring(1);
        }

        Var var = findVariable(actualIdent);
        if (var != null) {
            if (!exec.buildSql) {
                if (hasSub) {
                    Var var1 = new Var(var);
                    var1.negate();
                    exec.stackPush(var1);
                } else {
                    exec.stackPush(var);
                }
            } else {
                exec.stackPush(new Var(ident, Var.Type.STRING, var.toSqlString()));
            }
        } else {
            if (exec.buildSql || exec.inCallStmt) {
                exec.stackPush(new Var(Var.Type.IDENT, ident));
            } else {
                ident = ident.toUpperCase();
                if (!exec.functions.exec(ident, null)) {
                    throw new UndefinedIdentException(ctx, ident);
                }
            }
        }
        return 0;
    }

    /**
     * Single quoted string literal
     */
    @Override
    public Integer visitSingle_quotedString(org.apache.doris.hplsql.HplsqlParser.Single_quotedStringContext ctx) {
        if (exec.buildSql) {
            exec.stackPush(ctx.getText());
        } else {
            exec.stackPush(Utils.unquoteString(ctx.getText()));
        }
        return 0;
    }

    /**
     * Integer literal, signed or unsigned
     */
    @Override
    public Integer visitInt_number(org.apache.doris.hplsql.HplsqlParser.Int_numberContext ctx) {
        exec.stack.push(new Var(Long.valueOf(ctx.getText())));
        return 0;
    }

    /**
     * Interval expression (INTERVAL '1' DAY i.e)
     */
    @Override
    public Integer visitExpr_interval(org.apache.doris.hplsql.HplsqlParser.Expr_intervalContext ctx) {
        int num = evalPop(ctx.expr()).intValue();
        Interval interval = new Interval().set(num, ctx.interval_item().getText());
        stackPush(new Var(interval));
        return 0;
    }

    /**
     * Decimal literal, signed or unsigned
     */
    @Override
    public Integer visitDec_number(org.apache.doris.hplsql.HplsqlParser.Dec_numberContext ctx) {
        stackPush(new Var(new BigDecimal(ctx.getText())));
        return 0;
    }

    /**
     * Boolean literal
     */
    @Override
    public Integer visitBool_literal(org.apache.doris.hplsql.HplsqlParser.Bool_literalContext ctx) {
        boolean val = true;
        if (ctx.T_FALSE() != null) {
            val = false;
        }
        stackPush(new Var(val));
        return 0;
    }

    /**
     * NULL constant
     */
    @Override
    public Integer visitNull_const(org.apache.doris.hplsql.HplsqlParser.Null_constContext ctx) {
        stackPush(new Var());
        return 0;
    }

    /**
     * DATE 'YYYY-MM-DD' literal
     */
    @Override
    public Integer visitDate_literal(org.apache.doris.hplsql.HplsqlParser.Date_literalContext ctx) {
        if (!exec.buildSql) {
            String str = evalPop(ctx.string()).toString();
            stackPush(new Var(Var.Type.DATE, Utils.toDate(str)));
        } else {
            stackPush(getFormattedText(ctx));
        }
        return 0;
    }

    /**
     * TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFF' literal
     */
    @Override
    public Integer visitTimestamp_literal(org.apache.doris.hplsql.HplsqlParser.Timestamp_literalContext ctx) {
        if (!exec.buildSql) {
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
        } else {
            stackPush(getFormattedText(ctx));
        }
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
        if (exec.stmtConnList.contains(exec.conf.defaultConnection)) {
            return exec.conf.defaultConnection;
        } else if (!exec.stmtConnList.isEmpty()) {
            return exec.stmtConnList.get(0);
        }
        return exec.conf.defaultConnection;
    }

    /**
     * Define the connection profile for the specified object
     *
     * @return
     */
    String getObjectConnection(String name) {
        String conn = exec.objectConnMap.get(name.toUpperCase());
        if (conn != null) {
            return conn;
        }
        return exec.conf.defaultConnection;
    }

    /**
     * Get the connection (open the new connection if not available)
     *
     * @throws Exception
     */
    Connection getConnection(String conn) throws Exception {
        if (conn == null || conn.equalsIgnoreCase("default")) {
            conn = exec.conf.defaultConnection;
        }
        return exec.conn.getConnection(conn);
    }

    /**
     * Return the connection to the pool
     */
    void returnConnection(String name, Connection conn) {
        exec.conn.returnConnection(name, conn);
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
     * Add managed temporary table
     */
    public void addManagedTable(String name, String managedName) {
        exec.managedTables.put(name, managedName);
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
    String evalPop(org.apache.doris.hplsql.HplsqlParser.DtypeContext type,
            org.apache.doris.hplsql.HplsqlParser.Dtype_lenContext len) {
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
            console.printError("Ln:" + ctx.getStart().getLine() + " " + message);
        } else {
            console.printError(message);
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
