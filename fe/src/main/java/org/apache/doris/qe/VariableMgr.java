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
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SysVariableDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;

import org.apache.commons.lang.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Variable manager, merge session variable and global variable
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
    private static ImmutableMap<String, VarContext> ctxByVarName;

    // global session variable
    private static SessionVariable globalSessionVariable;

    // Global read/write lock to protect access global variable.
    private static final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private static final Lock rlock = rwlock.readLock();
    private static final Lock wlock = rwlock.writeLock();

    // Form map from variable name to its field in Java class.
    static {
        // Session value
        globalSessionVariable = new SessionVariable();
        ImmutableSortedMap.Builder<String, VarContext> builder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }

            field.setAccessible(true);
            builder.put(attr.name(),
                    new VarContext(field, globalSessionVariable, SESSION | attr.flag(),
                            getValue(globalSessionVariable, field)));
        }

        // Variables only exist in global environment.
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                LOG.warn("Field in GlobalVariable with VarAttr annotation must have static modifier.");
                continue;
            }

            field.setAccessible(true);
            builder.put(attr.name(),
                    new VarContext(field, null, GLOBAL | attr.flag(), getValue(null, field)));

        }

        ctxByVarName = builder.build();
    }

    public static Lock readLock() {
        return rlock;
    }

    public static Lock writeLock() {
        return wlock;
    }

    public static SessionVariable getGlobalSessionVariable() {
        return globalSessionVariable;
    }

    // Set value to a variable
    private static boolean setValue(Object obj, Field field, String value) throws DdlException {
        VarAttr attr = field.getAnnotation(VarAttr.class);
        String convertedVal = VariableVarConverters.convert(attr.name(), value);
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    if (convertedVal.equalsIgnoreCase("ON")
                            || convertedVal.equalsIgnoreCase("TRUE")
                            || convertedVal.equalsIgnoreCase("1")) {
                        field.setBoolean(obj, true);
                    } else if (convertedVal.equalsIgnoreCase("OFF")
                            || convertedVal.equalsIgnoreCase("FALSE")
                            || convertedVal.equalsIgnoreCase("0")) {
                        field.setBoolean(obj, false);
                    } else {
                        throw new IllegalAccessException();
                    }
                    break;
                case "byte":
                    field.setByte(obj, Byte.valueOf(convertedVal));
                    break;
                case "short":
                    field.setShort(obj, Short.valueOf(convertedVal));
                    break;
                case "int":
                    field.setInt(obj, Integer.valueOf(convertedVal));
                    break;
                case "long":
                    field.setLong(obj, Long.valueOf(convertedVal));
                    break;
                case "float":
                    field.setFloat(obj, Float.valueOf(convertedVal));
                    break;
                case "double":
                    field.setDouble(obj, Double.valueOf(convertedVal));
                    break;
                case "String":
                    field.set(obj, convertedVal);
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

        return true;
    }

    public static SessionVariable newSessionVariable() {
        wlock.lock();
        try {
            return (SessionVariable) SerializationUtils.clone(globalSessionVariable);
        } finally {
            wlock.unlock();
        }
    }

    // Check if this setVar can
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

    // Get from show name to field
    public static void setVar(SessionVariable sessionVariable, SetVar setVar) throws DdlException {
        VarContext ctx = ctxByVarName.get(setVar.getVariable());
        if (ctx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, setVar.getVariable());
        }
        // Check variable attribute and setVar
        checkUpdate(setVar, ctx.getFlag());
        // Check variable time_zone value is valid
        if (setVar.getVariable().toLowerCase().equals("time_zone")) {
            setVar = new SetVar(
                    setVar.getType(), setVar.getVariable(),
                    new StringLiteral(TimeUtils.checkTimeZoneValidAndStandardize(setVar.getValue().getStringValue())));
        }
        if (setVar.getVariable().toLowerCase().equals("exec_mem_limit")) {
            try {
            setVar = new SetVar(
                    setVar.getType(), setVar.getVariable(),
                    new StringLiteral(Long.toString(ParseUtil.analyzeDataVolumn(setVar.getValue().getStringValue()))));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

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
            wlock.lock();
            try {
                setValue(ctx.getObj(), ctx.getField(), value);
            } finally {
                wlock.unlock();
            }
            writeGlobalVariableUpdate(globalSessionVariable, "update global variables");
        } else {
            // set global variable should not affect variables of current session.
            // global variable will only make effect when connecting in.
            setValue(sessionVariable, ctx.getField(), value);
        }
    }

    // global variable persistence
    public static void write(DataOutputStream out) throws IOException {
        globalSessionVariable.write(out);
    }
    
    public static void read(DataInputStream in) throws IOException, DdlException {
        wlock.lock();
        try {
            globalSessionVariable.readFields(in);
        } finally {
            wlock.unlock();
        }
    }

    private static void writeGlobalVariableUpdate(SessionVariable variable, String msg) {
        EditLog editLog = Catalog.getCurrentCatalog().getEditLog();
        editLog.logGlobalVariable(variable);
    }

    public static void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        wlock.lock();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }

                field.setAccessible(true);

                VarContext ctx = ctxByVarName.get(attr.name());
                if (ctx.getFlag() == SESSION) {
                    String value = getValue(variable, ctx.getField());
                    setValue(ctx.getObj(), ctx.getField(), value);
                }
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

    // Dump all fields
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
                if (type != SetType.GLOBAL && ctx.getObj() == globalSessionVariable) {
                    // In this condition, we may retrieve session variables for caller.
                    row.add(getValue(sessionVar, ctx.getField()));
                } else {
                    row.add(getValue(ctx.getObj(), ctx.getField()));
                }

                if (row.size() > 1 && row.get(0).equalsIgnoreCase(SessionVariable.SQL_MODE)) {
                    try {
                        row.set(1, SqlModeHelper.decode(Long.valueOf(row.get(1))));
                    } catch (DdlException e) {
                        row.set(1, "");
                        LOG.warn("Decode sql mode failed");
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
}
