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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Function.NullableMode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts {@link Function} and its subclasses to their SQL representations.
 */
public class FunctionToSqlConverter {

    /**
     * Converts a {@link Function} (or subclass) to its SQL representation.
     * Uses instanceof checks to dispatch to the appropriate subclass handler.
     */
    public static String toSql(Function fn, boolean ifNotExists) {
        if (fn instanceof AliasFunction) {
            return toSql((AliasFunction) fn, ifNotExists);
        } else if (fn instanceof ScalarFunction) {
            return toSql((ScalarFunction) fn, ifNotExists);
        } else if (fn instanceof AggregateFunction) {
            return toSql((AggregateFunction) fn, ifNotExists);
        }
        return "";
    }

    /**
     * Converts a {@link ScalarFunction} to its SQL representation.
     */
    public static String toSql(ScalarFunction fn, boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (fn.isGlobal()) {
            sb.append("GLOBAL ");
        }
        sb.append("FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(fn.signatureString())
                .append(" RETURNS " + fn.getReturnType())
                .append(" PROPERTIES (");
        sb.append("\n  \"SYMBOL\"=").append("\"" + fn.getSymbolName() + "\"");
        if (fn.getPrepareFnSymbol() != null) {
            sb.append(",\n  \"PREPARE_FN\"=").append("\"" + fn.getPrepareFnSymbol() + "\"");
        }
        if (fn.getCloseFnSymbol() != null) {
            sb.append(",\n  \"CLOSE_FN\"=").append("\"" + fn.getCloseFnSymbol() + "\"");
        }

        if (fn.getBinaryType() == Function.BinaryType.JAVA_UDF) {
            sb.append(",\n  \"FILE\"=")
                    .append("\"" + (fn.getLocation() == null ? "" : fn.getLocation().toString()) + "\"");
            boolean isReturnNull = fn.getNullableMode() == NullableMode.ALWAYS_NULLABLE;
            sb.append(",\n  \"ALWAYS_NULLABLE\"=").append("\"" + isReturnNull + "\"");
        } else {
            sb.append(",\n  \"OBJECT_FILE\"=")
                    .append("\"" + (fn.getLocation() == null ? "" : fn.getLocation().toString()) + "\"");
        }
        sb.append(",\n  \"TYPE\"=").append("\"" + fn.getBinaryType() + "\"");
        sb.append("\n);");
        return sb.toString();
    }

    /**
     * Converts an {@link AggregateFunction} to its SQL representation.
     */
    public static String toSql(AggregateFunction fn, boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE ");

        if (fn.isGlobal()) {
            sb.append("GLOBAL ");
        }
        sb.append("AGGREGATE FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        sb.append(fn.signatureString()).append(" RETURNS " + fn.getReturnType());
        if (fn.getIntermediateType() != null) {
            sb.append(" INTERMEDIATE " + fn.getIntermediateType());
        }

        sb.append(" PROPERTIES (");
        if (fn.getBinaryType() != Function.BinaryType.JAVA_UDF) {
            sb.append("\n  \"INIT_FN\"=\"" + fn.getInitFnSymbol() + "\",")
                    .append("\n  \"UPDATE_FN\"=\"" + fn.getUpdateFnSymbol() + "\",")
                    .append("\n  \"MERGE_FN\"=\"" + fn.getMergeFnSymbol() + "\",");
            if (fn.getSerializeFnSymbol() != null) {
                sb.append("\n  \"SERIALIZE_FN\"=\"" + fn.getSerializeFnSymbol() + "\",");
            }
            if (fn.getFinalizeFnSymbol() != null) {
                sb.append("\n  \"FINALIZE_FN\"=\"" + fn.getFinalizeFnSymbol() + "\",");
            }
        }
        if (fn.getSymbolName() != null) {
            sb.append("\n  \"SYMBOL\"=\"" + fn.getSymbolName() + "\",");
        }

        if (fn.getBinaryType() == Function.BinaryType.JAVA_UDF) {
            sb.append("\n  \"FILE\"=")
                    .append("\"" + (fn.getLocation() == null ? "" : fn.getLocation().toString()) + "\",");
            boolean isReturnNull = fn.getNullableMode() == NullableMode.ALWAYS_NULLABLE;
            sb.append("\n  \"ALWAYS_NULLABLE\"=").append("\"" + isReturnNull + "\",");
        } else {
            sb.append("\n  \"OBJECT_FILE\"=")
                    .append("\"" + (fn.getLocation() == null ? "" : fn.getLocation().toString()) + "\",");
        }
        sb.append("\n  \"TYPE\"=").append("\"" + fn.getBinaryType() + "\"");
        sb.append("\n);");
        return sb.toString();
    }

    /**
     * Converts an {@link AliasFunction} to its SQL representation.
     */
    public static String toSql(AliasFunction fn, boolean ifNotExists) {
        setSlotRefLabel(fn.getOriginFunction());
        StringBuilder sb = new StringBuilder("CREATE ");

        if (fn.isGlobal()) {
            sb.append("GLOBAL ");
        }
        sb.append("ALIAS FUNCTION ");

        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(fn.signatureString())
                .append(" WITH PARAMETER(")
                .append(getParamsSting(fn.getParameters()))
                .append(") AS ")
                .append(fn.getOriginFunction().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE))
                .append(";");
        return sb.toString();
    }

    private static void setSlotRefLabel(Expr expr) {
        for (Expr e : expr.getChildren()) {
            setSlotRefLabel(e);
        }
        if (expr instanceof SlotRef) {
            ((SlotRef) expr).setLabel("`" + ((SlotRef) expr).getColumnName() + "`");
        }
    }

    private static String getParamsSting(List<String> parameters) {
        return parameters.stream()
                .map(String::toString)
                .collect(Collectors.joining(", "));
    }
}
