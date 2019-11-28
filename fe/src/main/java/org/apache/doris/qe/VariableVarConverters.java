package org.apache.doris.qe;

import com.google.common.collect.Maps;

import java.util.Map;

public class VariableVarConverters {

    public static final Map<String, VariableVarConverterI> converters = Maps.newHashMap();
    static {
        SqlModeConverter sqlModeConverter = new SqlModeConverter();
        converters.put(SessionVariable.SQL_MODE, sqlModeConverter);
    }

    public static String convert(String varName, String value) {
        if (converters.containsKey(varName)) {
            return converters.get(varName).convert(value);
        }
        return value;
    }

    public static class SqlModeConverter implements VariableVarConverterI {
        @Override
        public String convert(String value) {
            return "0";
        }
    }
}
