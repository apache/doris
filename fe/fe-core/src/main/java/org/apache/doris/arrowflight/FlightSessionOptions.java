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

package org.apache.doris.arrowflight;

import org.apache.doris.analysis.SetType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.nereids.util.SqlLiteralUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VarAttrDef;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.VariableMgr.VarContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SessionOptionValueVisitor;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.SetSessionOptionsResult.ErrorValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The session options of an Arrow Flight SQL session, as the SetSessionOptions and GetSessionOptions
 * actions see them.
 *
 * <p>An option stands for the statement of the session that sets it: {@code catalog} is the current
 * catalog ({@code SWITCH}), {@code schema} the current database ({@code USE}), and any other name is
 * the session variable of that name ({@code SET SESSION}). Those two names are what the ADBC Flight
 * SQL driver puts on the wire for {@code adbc.connection.catalog} and
 * {@code adbc.connection.db_schema}, and what the Flight SQL JDBC driver sends for its
 * {@code catalog} property; every other option name the drivers pass through as given. Setting an
 * option runs its statement as a command of the session, so it is checked, audited and takes effect
 * exactly as if the client had sent the statement. Reading the options back gives what
 * {@code SHOW VARIABLES} shows, every value as a string: the one representation {@code SET} accepts
 * back, whatever the Java type of the variable behind it.
 *
 * <p>The result of setting an option is one of the three {@link ErrorValue}s per name and nothing
 * else, so the reason a value was refused only reaches the frontend log.
 */
public final class FlightSessionOptions {
    private static final Logger LOG = LogManager.getLogger(FlightSessionOptions.class);

    /** The current catalog; set with {@code SWITCH}. */
    public static final String CATALOG = "catalog";
    /** The current database; set with {@code USE}. */
    public static final String SCHEMA = "schema";

    // What a session variable is called in SET: VariableMgr looks the name up case-insensitively,
    // and only a name made of these characters is one the parser reads as the variable's identifier.
    private static final Pattern VARIABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private FlightSessionOptions() {
    }

    /**
     * Sets the options of one request, each on its own, and returns the error of every option that
     * could not be set; an option absent from the result was set. {@code catalog} goes first and
     * {@code schema} second, since {@code USE} names a database of the current catalog; the others
     * follow in name order, so that a request applies the same way every time. Runs as a command of
     * the session, under its command lock.
     */
    public static Map<String, SetSessionOptionsResult.Error> set(ConnectContext ctx,
            Map<String, SessionOptionValue> options) {
        List<String> names = new ArrayList<>(options.keySet());
        Collections.sort(names);
        names.remove(SCHEMA);
        names.remove(CATALOG);
        if (options.containsKey(SCHEMA)) {
            names.add(0, SCHEMA);
        }
        if (options.containsKey(CATALOG)) {
            names.add(0, CATALOG);
        }
        Map<String, SetSessionOptionsResult.Error> errors = new LinkedHashMap<>();
        for (String name : names) {
            ErrorValue error = setOne(ctx, name, options.get(name));
            if (error != null) {
                errors.put(name, new SetSessionOptionsResult.Error(error));
            }
        }
        return errors;
    }

    /** Sets one option and returns why it could not be, or null when it was set. */
    @VisibleForTesting
    static ErrorValue setOne(ConnectContext ctx, String name, SessionOptionValue value) {
        if (CATALOG.equals(name)) {
            return switchCatalog(ctx, value);
        }
        if (SCHEMA.equals(name)) {
            return useDatabase(ctx, value);
        }
        return setVariable(ctx, name, value);
    }

    /**
     * The options of the session: the current catalog, the current database (an empty string when
     * none has been chosen) and every session variable {@code SHOW VARIABLES} would list, in its text.
     */
    public static Map<String, SessionOptionValue> get(ConnectContext ctx) {
        Map<String, SessionOptionValue> options = new LinkedHashMap<>();
        options.put(CATALOG, SessionOptionValueFactory.makeSessionOptionValue(ctx.getDefaultCatalog()));
        String database = ctx.getDatabase();
        options.put(SCHEMA, SessionOptionValueFactory.makeSessionOptionValue(database == null ? "" : database));
        for (List<String> row : VariableMgr.dump(SetType.SESSION, ctx.getSessionVariable(), null)) {
            // A row is name, value, default value, changed.
            options.put(row.get(0), SessionOptionValueFactory.makeSessionOptionValue(row.get(1)));
        }
        return options;
    }

    private static ErrorValue switchCatalog(ConnectContext ctx, SessionOptionValue value) {
        String catalog = value.acceptVisitor(STRING_VALUE);
        if (catalog == null || catalog.isEmpty()) {
            return ErrorValue.INVALID_VALUE;
        }
        return runStatement(ctx, CATALOG, "SWITCH " + quoteIdentifier(catalog), ErrorCode.ERR_UNKNOWN_CATALOG);
    }

    private static ErrorValue useDatabase(ConnectContext ctx, SessionOptionValue value) {
        String database = value.acceptVisitor(STRING_VALUE);
        if (database == null || database.isEmpty()) {
            return ErrorValue.INVALID_VALUE;
        }
        return runStatement(ctx, SCHEMA, "USE " + quoteIdentifier(database), ErrorCode.ERR_BAD_DB_ERROR);
    }

    private static ErrorValue setVariable(ConnectContext ctx, String name, SessionOptionValue value) {
        // Only a session variable is a session option. A name SET quietly ignores for the sake of
        // MySQL clients (a removed variable, the MySQL compatibility whitelist) is no variable of
        // this session either: setting it would set nothing, and reading the options back would not
        // list it.
        if (!VARIABLE_NAME.matcher(name).matches()) {
            return ErrorValue.INVALID_NAME;
        }
        VarContext varCtx = VariableMgr.getVarContext(name);
        if (varCtx == null) {
            return ErrorValue.INVALID_NAME;
        }
        // SET SESSION refuses a read-only variable and one that exists once per frontend rather than
        // per session (SET GLOBAL sets that one); neither can be set as a session option.
        if ((varCtx.getFlag() & (VarAttrDef.READ_ONLY | VarAttrDef.GLOBAL)) != 0) {
            return ErrorValue.ERROR;
        }
        // A string is quoted for the session's sql_mode, the mode the statement is then parsed under.
        String literal = SqlModeHelper.withSqlMode(ctx.getSessionVariable().getSqlMode(),
                () -> value.acceptVisitor(SET_LITERAL));
        if (literal == null) {
            return ErrorValue.INVALID_VALUE;
        }
        // The name and the scope were checked above, so what is left for SET to refuse is the value:
        // its type, its range, or what a variable's own checker makes of it.
        return runStatement(ctx, name, "SET SESSION " + name + " = " + literal, null);
    }

    /**
     * Runs the statement an option stands for as a command of the session. A failure is
     * {@code INVALID_VALUE} when the value was refused and {@code ERROR} otherwise. For SWITCH and
     * USE the two are told apart by the error code the statement left on the session: an unknown
     * catalog or database is written there by the command itself, while a statement that fails in
     * validation, e.g. for lack of privilege, leaves the executor's generic code. For SET every
     * failure is the value's (see {@link #setVariable}), so {@code refusedValueCode} is null there.
     */
    private static ErrorValue runStatement(ConnectContext ctx, String option, String statement,
            ErrorCode refusedValueCode) {
        try (FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(ctx)) {
            processor.handleQuery(statement);
            if (ctx.getState().getStateType() != MysqlStateType.ERR) {
                return null;
            }
            LOG.warn("session option {} of Arrow Flight SQL connection {} could not be set, statement: {}, "
                    + "error code: {}, error message: {}", option, ctx.getConnectionId(), statement,
                    ctx.getState().getErrorCode(), ctx.getState().getErrorMessage());
            if (refusedValueCode == null || refusedValueCode == ctx.getState().getErrorCode()) {
                return ErrorValue.INVALID_VALUE;
            }
            return ErrorValue.ERROR;
        } catch (Throwable e) {
            LOG.warn("session option {} of Arrow Flight SQL connection {} could not be set, statement: {}",
                    option, ctx.getConnectionId(), statement, e);
            return ErrorValue.ERROR;
        }
    }

    private static String quoteIdentifier(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /** The string of a string-valued option; null for any other kind of value. */
    private static final SessionOptionValueVisitor<String> STRING_VALUE = new SessionOptionValueVisitor<String>() {
        @Override
        public String visit(String value) {
            return value;
        }

        @Override
        public String visit(boolean value) {
            return null;
        }

        @Override
        public String visit(long value) {
            return null;
        }

        @Override
        public String visit(double value) {
            return null;
        }

        @Override
        public String visit(String[] value) {
            return null;
        }

        @Override
        public String visit(Void value) {
            return null;
        }
    };

    /**
     * What an option value is as the right-hand side of {@code SET SESSION name = }: a literal of the
     * value's own type, {@code DEFAULT} for the empty value that unsets an option, and null for a
     * value that has no such form -- a list, or a double that is not a number.
     */
    private static final SessionOptionValueVisitor<String> SET_LITERAL = new SessionOptionValueVisitor<String>() {
        @Override
        public String visit(String value) {
            return SqlLiteralUtils.quoteStringLiteral(value);
        }

        @Override
        public String visit(boolean value) {
            return value ? "true" : "false";
        }

        @Override
        public String visit(long value) {
            return Long.toString(value);
        }

        @Override
        public String visit(double value) {
            return Double.isFinite(value) ? Double.toString(value) : null;
        }

        @Override
        public String visit(String[] value) {
            return null;
        }

        @Override
        public String visit(Void value) {
            return "DEFAULT";
        }
    };
}
