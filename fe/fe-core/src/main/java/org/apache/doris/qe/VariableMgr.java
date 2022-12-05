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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.SysVariableDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.persist.GlobalVarPersistInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Variable manager, merge session variable and global variable.
 * <p>
 * There are two types of variables, SESSION and GLOBAL.
 * <p>
 * The GLOBAL variable is more like a system configuration, which takes effect globally.
 * The settings for global variables are global and persistent.
 * After the cluster is restarted, the set values ​​can still be restored.
 * The global variables are defined in `GlobalVariable`.
 * The variable of the READ_ONLY attribute cannot be changed,
 * and the variable of the GLOBAL attribute can be changed at runtime.
 * <p>
 * Session variables are session-level, and the scope of these variables is usually
 * in a session connection. The session variables are defined in `SessionVariable`.
 * <p>
 * For the setting of the global variable, the value of the field in the `GlobalVariable` class
 * will be modified directly through the reflection mechanism of Java.
 * <p>
 * For the setting of session variables, there are also two types: Global and Session.
 * <p>
 * 1. Use `set global` comment to set session variables
 * <p>
 * This setting method is equivalent to changing the default value of the session variable.
 * It will modify the `defaultSessionVariable` member.
 * This operation is persistent and global. After the setting is complete, when a new session
 * is established, this default value will be used to generate session-level session variables.
 * This operation will only affect the value of the variable in the newly established session,
 * but will not affect the value of the variable in the current session.
 * <p>
 * 2. Use the `set` comment (no global) to set the session variable
 * <p>
 * This setting method will only change the value of the variable in the current session.
 * After the session ends, this setting will also become invalid.
 */
public class VariableMgr {
    private static final Logger LOG = LogManager.getLogger(VariableMgr.class);

    // variable have this flag means that every session have a copy of this variable,
    // and can modify its own variable.
    public static final int SESSION = 1;
    // Variables with this flag have only one instance in one process.
    public static final int GLOBAL = 2;
    // Variables with this flag only exist in each session.
    public static final int SESSION_ONLY = 4;
    // Variables with this flag can only be read.
    public static final int READ_ONLY = 8;
    // Variables with this flag can not be seen with `SHOW VARIABLES` statement.
    public static final int INVISIBLE = 16;

    // Map variable name to variable context which have enough information to change variable value.
    // This map contains info of all session and global variables.
    private static ImmutableMap<String, VarContext> ctxByVarName;

    // This variable is equivalent to the default value of session variables.
    // Whenever a new session is established, the value in this object is copied to the session-level variable.
    private static SessionVariable defaultSessionVariable;

    // The following 2 static fields is for checkpoint.
    // Because ctxByVarName and defaultSessionVariable are static variables, and during the checkpoint process,
    // we cannot modify any values in Serving Catalog, including these static variables.
    // So we use two additional fields provided to the checkpoint thread.
    private static SessionVariable defaultSessionVariableForCkpt;
    private static ImmutableMap<String, VarContext> ctxByVarNameForCkpt;

    // Global read/write lock to protect access of globalSessionVariable.
    private static final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private static final Lock rlock = rwlock.readLock();
    private static final Lock wlock = rwlock.writeLock();

    // Form map from variable name to its field in Java class.
    static {
        // Session value
        defaultSessionVariable = new SessionVariable();
        ImmutableSortedMap.Builder<String, VarContext> builder = getStringVarContextBuilder(defaultSessionVariable);
        ctxByVarName = builder.build();
    }

    public static SessionVariable getDefaultSessionVariable() {
        return defaultSessionVariable;
    }

    // Set value to a variable
    private static boolean setValue(Object obj, Field field, String value) throws DdlException {
        VarAttr attr = field.getAnnotation(VarAttr.class);
        if (VariableVarConverters.hasConverter(attr.name())) {
            value = VariableVarConverters.encode(attr.name(), value).toString();
        }
        if (!attr.checker().equals("")) {
            Preconditions.checkArgument(obj instanceof SessionVariable);
            try {
                SessionVariable.class.getDeclaredMethod(attr.checker()).invoke(obj);
            } catch (Exception e) {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, attr.name(), value, e.getMessage());
            }
        }
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    if (value.equalsIgnoreCase("ON")
                            || value.equalsIgnoreCase("TRUE")
                            || value.equalsIgnoreCase("1")) {
                        field.setBoolean(obj, true);
                    } else if (value.equalsIgnoreCase("OFF")
                            || value.equalsIgnoreCase("FALSE")
                            || value.equalsIgnoreCase("0")) {
                        field.setBoolean(obj, false);
                    } else {
                        throw new IllegalAccessException();
                    }
                    break;
                case "byte":
                    field.setByte(obj, Byte.valueOf(value));
                    break;
                case "short":
                    field.setShort(obj, Short.valueOf(value));
                    break;
                case "int":
                    field.setInt(obj, Integer.valueOf(value));
                    break;
                case "long":
                    field.setLong(obj, Long.valueOf(value));
                    break;
                case "float":
                    field.setFloat(obj, Float.valueOf(value));
                    break;
                case "double":
                    field.setDouble(obj, Double.valueOf(value));
                    break;
                case "String":
                    field.set(obj, value);
                    break;
                default:
                    // Unsupported type variable.
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, attr.name());
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, attr.name());
        } catch (IllegalAccessException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, attr.name(), value);
        }

        if (VariableVarCallbacks.hasCallback(attr.name())) {
            VariableVarCallbacks.call(attr.name(), value);
        }

        return true;
    }

    // revert the operator[set_var] on select/*+ SET_VAR()*/  sql;
    public static void revertSessionValue(SessionVariable obj) throws DdlException {
        Map<Field, String> sessionOriginValue = obj.getSessionOriginValue();
        if (!sessionOriginValue.isEmpty()) {
            for (Field field : sessionOriginValue.keySet()) {
                // revert session value
                setValue(obj, field, sessionOriginValue.get(field));
            }
        }
    }

    public static SessionVariable newSessionVariable() {
        wlock.lock();
        try {
            return (SessionVariable) SerializationUtils.clone(defaultSessionVariable);
        } finally {
            wlock.unlock();
        }
    }

    // Check if this setVar can be set correctly
    private static void checkUpdate(SetVar setVar, int flag) throws DdlException {
        if ((flag & READ_ONLY) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_VARIABLE_IS_READONLY, setVar.getVariable());
        }
        if (setVar.getType() == SetType.GLOBAL && (flag & SESSION_ONLY) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_LOCAL_VARIABLE, setVar.getVariable());
        }
        if (setVar.getType() != SetType.GLOBAL && (flag & GLOBAL) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_GLOBAL_VARIABLE, setVar.getVariable());
        }
    }

    // Entry of handling SetVarStmt
    // Input:
    //      sessionVariable: the variable of current session
    //      setVar: variable information that needs to be set
    public static void setVar(SessionVariable sessionVariable, SetVar setVar) throws DdlException {
        VarContext ctx = ctxByVarName.get(setVar.getVariable());
        if (ctx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, setVar.getVariable());
        }
        // Check variable attribute and setVar
        checkUpdate(setVar, ctx.getFlag());

        // To modify to default value.
        VarAttr attr = ctx.getField().getAnnotation(VarAttr.class);
        String value;
        // If value is null, this is `set variable = DEFAULT`
        if (setVar.getValue() != null) {
            value = setVar.getValue().getStringValue();
        } else {
            value = ctx.getDefaultValue();
            if (value == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_DEFAULT, attr.name());
            }
        }

        if (setVar.getType() == SetType.GLOBAL) {
            setGlobalVarAndWriteEditLog(ctx, attr.name(), setVar.getValue().getStringValue());
        }

        // No matter this is a global setting or not, always set session variable.
        Field field = ctx.getField();
        // if stmt is "Select /*+ SET_VAR(...)*/"
        if (sessionVariable.getIsSingleSetVar()) {
            try {
                sessionVariable.addSessionOriginValue(field, field.get(sessionVariable).toString());
            } catch (Exception e) {
                LOG.warn("failed to collect origin session value ", e);
            }
        }
        setValue(sessionVariable, field, value);
    }

    private static void setGlobalVarAndWriteEditLog(VarContext ctx, String name, String value) throws DdlException {
        // global variable will make effect when is set immediately.
        wlock.lock();
        try {
            setValue(ctx.getObj(), ctx.getField(), value);
            // write edit log
            GlobalVarPersistInfo info = new GlobalVarPersistInfo(defaultSessionVariable, Lists.newArrayList(name));
            Env.getCurrentEnv().getEditLog().logGlobalVariableV2(info);
        } finally {
            wlock.unlock();
        }
    }

    public static void setLowerCaseTableNames(int mode) throws DdlException {
        VarContext ctx = ctxByVarName.get(GlobalVariable.LOWER_CASE_TABLE_NAMES);
        setGlobalVarAndWriteEditLog(ctx, GlobalVariable.LOWER_CASE_TABLE_NAMES, "" + mode);
    }

    // global variable persistence
    public static void write(DataOutputStream out) throws IOException {
        SessionVariable variablesToWrite = defaultSessionVariable;
        if (Env.isCheckpointThread()) {
            // If this is checkpoint thread, we should write value in `defaultSessionVariableForCkpt` to the image
            // instead of `defaultSessionVariable`.
            variablesToWrite = defaultSessionVariableForCkpt;
        }
        variablesToWrite.write(out);
        // get all global variables
        List<String> varNames = GlobalVariable.getPersistentGlobalVarNames();
        GlobalVarPersistInfo info = new GlobalVarPersistInfo(variablesToWrite, varNames);
        info.write(out);
    }

    public static void read(DataInputStream in) throws IOException, DdlException {
        wlock.lock();
        try {
            SessionVariable variablesToRead = defaultSessionVariable;
            if (Env.isCheckpointThread()) {
                // If this is checkpoint thread, we should read value to set them to `defaultSessionVariableForCkpt`
                // instead of `defaultSessionVariable`.
                // This approach ensures that checkpoint threads do not modify the values in serving catalog.
                variablesToRead = defaultSessionVariableForCkpt;
            }
            variablesToRead.readFields(in);
            GlobalVarPersistInfo info = GlobalVarPersistInfo.read(in);
            replayGlobalVariableV2(info);
        } finally {
            wlock.unlock();
        }
    }

    // this method is used to replace the `replayGlobalVariable()`
    public static void replayGlobalVariableV2(GlobalVarPersistInfo info) throws DdlException {
        wlock.lock();
        try {
            String json = info.getPersistJsonString();
            JSONObject root = (JSONObject) JSONValue.parse(json);
            for (Object varName : root.keySet()) {
                VarContext varContext = ctxByVarName.get((String) varName);
                if (Env.isCheckpointThread()) {
                    // If this is checkpoint thread, we should write value in `ctxByVarNameForCkpt` to the image
                    // instead of `ctxByVarName`.
                    varContext = ctxByVarNameForCkpt.get((String) varName);
                }
                if (varContext == null) {
                    LOG.error("failed to get global variable {} when replaying", (String) varName);
                    continue;
                }
                setValue(varContext.getObj(), varContext.getField(), root.get((String) varName).toString());
            }
        } finally {
            wlock.unlock();
        }
    }

    // Get variable value through variable name, used to satisfy statement like `SELECT @@comment_version`
    public static void fillValue(SessionVariable var, SysVariableDesc desc) throws AnalysisException {
        VarContext ctx = ctxByVarName.get(desc.getName());
        if (ctx == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, desc.getName());
        }

        if (desc.getSetType() == SetType.GLOBAL) {
            rlock.lock();
            try {
                fillValue(ctx.getObj(), ctx.getField(), desc);
            } finally {
                rlock.unlock();
            }
        } else {
            fillValue(var, ctx.getField(), desc);
        }
    }

    private static void fillValue(Object obj, Field field, SysVariableDesc desc) {
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    desc.setType(Type.BOOLEAN);
                    desc.setBoolValue(field.getBoolean(obj));
                    break;
                case "byte":
                    desc.setType(Type.TINYINT);
                    desc.setIntValue(field.getByte(obj));
                    break;
                case "short":
                    desc.setType(Type.SMALLINT);
                    desc.setIntValue(field.getShort(obj));
                    break;
                case "int":
                    desc.setType(Type.INT);
                    desc.setIntValue(field.getInt(obj));
                    break;
                case "long":
                    desc.setType(Type.BIGINT);
                    desc.setIntValue(field.getLong(obj));
                    break;
                case "float":
                    desc.setType(Type.FLOAT);
                    desc.setFloatValue(field.getFloat(obj));
                    break;
                case "double":
                    desc.setType(Type.DOUBLE);
                    desc.setFloatValue(field.getDouble(obj));
                    break;
                case "String":
                    desc.setType(Type.VARCHAR);
                    desc.setStringValue((String) field.get(obj));
                    break;
                default:
                    desc.setType(Type.VARCHAR);
                    desc.setStringValue("");
                    break;
            }
        } catch (IllegalAccessException e) {
            LOG.warn("Access failed.", e);
        }
    }

    // Get variable value through variable name, used to satisfy statement like `SELECT @@comment_version`
    // For test only
    public static String getValue(SessionVariable var, SysVariableDesc desc) throws AnalysisException {
        VarContext ctx = ctxByVarName.get(desc.getName());
        if (ctx == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, desc.getName());
        }

        if (desc.getSetType() == SetType.GLOBAL) {
            rlock.lock();
            try {
                return getValue(ctx.getObj(), ctx.getField());
            } finally {
                rlock.unlock();
            }
        } else {
            return getValue(var, ctx.getField());
        }
    }

    private static String getValue(Object obj, Field field) {
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    return Boolean.toString(field.getBoolean(obj));
                case "byte":
                    return Byte.toString(field.getByte(obj));
                case "short":
                    return Short.toString(field.getShort(obj));
                case "int":
                    return Integer.toString(field.getInt(obj));
                case "long":
                    return Long.toString(field.getLong(obj));
                case "float":
                    return Float.toString(field.getFloat(obj));
                case "double":
                    return Double.toString(field.getDouble(obj));
                case "String":
                    return (String) field.get(obj);
                default:
                    return "";
            }
        } catch (IllegalAccessException e) {
            LOG.warn("Access failed.", e);
        }
        return "";
    }

    // Dump all fields. Used for `show variables`
    public static List<List<String>> dump(SetType type, SessionVariable sessionVar, PatternMatcher matcher) {
        List<List<String>> rows = Lists.newArrayList();
        // Hold the read lock when session dump, because this option need to access global variable.
        rlock.lock();
        try {
            for (Map.Entry<String, VarContext> entry : ctxByVarName.entrySet()) {
                // Filter variable not match to the regex.
                if (matcher != null && !matcher.match(entry.getKey())) {
                    continue;
                }
                VarContext ctx = entry.getValue();

                List<String> row = Lists.newArrayList();

                row.add(entry.getKey());
                if (type != SetType.GLOBAL && ctx.getObj() == defaultSessionVariable) {
                    // In this condition, we may retrieve session variables for caller.
                    row.add(getValue(sessionVar, ctx.getField()));
                } else {
                    row.add(getValue(ctx.getObj(), ctx.getField()));
                }

                if (row.size() > 1 && VariableVarConverters.hasConverter(row.get(0))) {
                    try {
                        row.set(1, VariableVarConverters.decode(row.get(0), Long.valueOf(row.get(1))));
                    } catch (DdlException e) {
                        row.set(1, "");
                        LOG.warn("Decode session variable failed");
                    }
                }

                rows.add(row);
            }
        } finally {
            rlock.unlock();
        }

        // Sort all variables by variable name.
        Collections.sort(rows, new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return o1.get(0).compareTo(o2.get(0));
            }
        });

        return rows;
    }

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface VarAttr {
        // Name in show variables and set statement;
        String name();

        int flag() default 0;

        // TODO(zhaochun): min and max is not used.
        String minValue() default "0";

        String maxValue() default "0";

        // the function name that check the VarAttr before setting it to sessionVariable
        // only support check function: 0 argument and 0 return value, if an error occurs, throw an exception.
        String checker() default "";

        // Set to true if the variables need to be forwarded along with forward statement.
        boolean needForward() default false;
    }

    private static class VarContext {
        private Field field;
        private Object obj;
        private int flag;
        private String defaultValue;

        public VarContext(Field field, Object obj, int flag, String defaultValue) {
            this.field = field;
            this.obj = obj;
            this.flag = flag;
            this.defaultValue = defaultValue;
        }

        public Field getField() {
            return field;
        }

        public Object getObj() {
            return obj;
        }

        public int getFlag() {
            return flag;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    public static void createDefaultSessionVariableForCkpt() {
        defaultSessionVariableForCkpt = new SessionVariable();
        ImmutableSortedMap.Builder<String, VarContext> builder
                = getStringVarContextBuilder(defaultSessionVariableForCkpt);
        ctxByVarNameForCkpt = builder.build();
    }

    public static void destroyDefaultSessionVariableForCkpt() {
        defaultSessionVariableForCkpt = null;
        ctxByVarNameForCkpt = null;
    }

    @NotNull
    private static ImmutableSortedMap.Builder<String, VarContext> getStringVarContextBuilder(
            SessionVariable sessionVariable) {
        ImmutableSortedMap.Builder<String, VarContext> builder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }

            field.setAccessible(true);
            builder.put(attr.name(),
                    new VarContext(field, sessionVariable, SESSION | attr.flag(),
                            getValue(sessionVariable, field)));
        }

        // Variables only exist in global environment.
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }

            field.setAccessible(true);
            builder.put(attr.name(), new VarContext(field, null, GLOBAL | attr.flag(), getValue(null, field)));
        }
        return builder;
    }
}
