/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Copied from com.zendesk:mysql-binlog-connector-java:0.27.2.
 *
 * <p>Lines 45 and 143-149: add vectorDimensionality and its accessors to retain
 * VECTOR dimension metadata.
 *
 * @author <a href="mailto:ahmedahamid@yahoo.com">Ahmed Abdul Hamid</a>
 */
public class TableMapEventMetadata implements EventData {

    private BitSet signedness;
    private DefaultCharset defaultCharset;
    private List<Integer> columnCharsets;
    private List<String> columnNames;
    private List<String[]> setStrValues;
    private List<String[]> enumStrValues;
    private List<Integer> geometryTypes;
    private List<Integer> simplePrimaryKeys;
    private Map<Integer, Integer> primaryKeysWithPrefix;
    private DefaultCharset enumAndSetDefaultCharset;
    private List<Integer> enumAndSetColumnCharsets;
    private BitSet visibility;
    private List<Integer> vectorDimensionality;

    public BitSet getSignedness() {
        return signedness;
    }

    public void setSignedness(BitSet signedness) {
        this.signedness = signedness;
    }

    public DefaultCharset getDefaultCharset() {
        return defaultCharset;
    }

    public void setDefaultCharset(DefaultCharset defaultCharset) {
        this.defaultCharset = defaultCharset;
    }

    public List<Integer> getColumnCharsets() {
        return columnCharsets;
    }

    public void setColumnCharsets(List<Integer> columnCharsets) {
        this.columnCharsets = columnCharsets;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<String[]> getSetStrValues() {
        return setStrValues;
    }

    public void setSetStrValues(List<String[]> setStrValues) {
        this.setStrValues = setStrValues;
    }

    public List<String[]> getEnumStrValues() {
        return enumStrValues;
    }

    public void setEnumStrValues(List<String[]> enumStrValues) {
        this.enumStrValues = enumStrValues;
    }

    public List<Integer> getGeometryTypes() {
        return geometryTypes;
    }

    public void setGeometryTypes(List<Integer> geometryTypes) {
        this.geometryTypes = geometryTypes;
    }

    public List<Integer> getSimplePrimaryKeys() {
        return simplePrimaryKeys;
    }

    public void setSimplePrimaryKeys(List<Integer> simplePrimaryKeys) {
        this.simplePrimaryKeys = simplePrimaryKeys;
    }

    public Map<Integer, Integer> getPrimaryKeysWithPrefix() {
        return primaryKeysWithPrefix;
    }

    public void setPrimaryKeysWithPrefix(Map<Integer, Integer> primaryKeysWithPrefix) {
        this.primaryKeysWithPrefix = primaryKeysWithPrefix;
    }

    public DefaultCharset getEnumAndSetDefaultCharset() {
        return enumAndSetDefaultCharset;
    }

    public void setEnumAndSetDefaultCharset(DefaultCharset enumAndSetDefaultCharset) {
        this.enumAndSetDefaultCharset = enumAndSetDefaultCharset;
    }

    public List<Integer> getEnumAndSetColumnCharsets() {
        return enumAndSetColumnCharsets;
    }

    public void setEnumAndSetColumnCharsets(List<Integer> enumAndSetColumnCharsets) {
        this.enumAndSetColumnCharsets = enumAndSetColumnCharsets;
    }

    public BitSet getVisibility() {
        return visibility;
    }

    public void setVisibility(BitSet visibility) {
        this.visibility = visibility;
    }

    public List<Integer> getVectorDimensionality() {
        return vectorDimensionality;
    }

    public void setVectorDimensionality(List<Integer> vectorDimensionality) {
        this.vectorDimensionality = vectorDimensionality;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TableMapEventMetadata");
        sb.append("{signedness=").append(signedness);
        sb.append(", defaultCharset=").append(defaultCharset == null ? "null" : defaultCharset);

        sb.append(", columnCharsets=").append(columnCharsets == null ? "null" : "");
        appendList(sb, columnCharsets);

        sb.append(", columnNames=").append(columnNames == null ? "null" : "");
        appendList(sb, columnNames);

        sb.append(", setStrValues=").append(setStrValues == null ? "null" : "");
        for (int i = 0; setStrValues != null && i < setStrValues.size(); ++i) {
            sb.append(i == 0 ? "" : ", ").append(join(", ", setStrValues.get(i)));
        }

        sb.append(", enumStrValues=").append(enumStrValues == null ? "null" : "");
        for (int i = 0; enumStrValues != null && i < enumStrValues.size(); ++i) {
            sb.append(i == 0 ? "" : ", ").append(join(", ", enumStrValues.get(i)));
        }

        sb.append(", geometryTypes=").append(geometryTypes == null ? "null" : "");
        appendList(sb, geometryTypes);

        sb.append(", simplePrimaryKeys=").append(simplePrimaryKeys == null ? "null" : "");
        appendList(sb, simplePrimaryKeys);

        sb.append(", primaryKeysWithPrefix=").append(primaryKeysWithPrefix == null ? "null" : "");
        appendMap(sb, primaryKeysWithPrefix);

        sb.append(", enumAndSetDefaultCharset=").append(enumAndSetDefaultCharset == null ? "null" :
            enumAndSetDefaultCharset);

        sb.append(", enumAndSetColumnCharsets=").append(enumAndSetColumnCharsets == null ? "null" : "");
        appendList(sb, enumAndSetColumnCharsets);

        sb.append(",visibility=").append(visibility);

        sb.append('}');
        return sb.toString();
    }

    private static String join(CharSequence delimiter, CharSequence... elements) {
        if (elements == null || elements.length == 0) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(elements[0]);

        for (int i = 1; i < elements.length; ++i) {
            sb.append(delimiter).append(elements[i]);
        }
        return sb.toString();
    }

    private static void appendList(StringBuilder sb, List<?> elements) {
        if (elements == null) {
            return;
        }

        for (int i = 0; i < elements.size(); ++i) {
            sb.append(i == 0 ? "" : ", ").append(elements.get(i));
        }
    }

    private static void appendMap(StringBuilder sb, Map<?, ?> map) {
        if (map == null) {
            return;
        }

        int entryCount = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            sb.append(entryCount++ == 0 ? "" : ", ").append(entry.getKey() + ": " + entry.getValue());
        }
    }

    /**
     * @author <a href="mailto:ahmedahamid@yahoo.com">Ahmed Abdul Hamid</a>
     */
    public static class DefaultCharset implements Serializable {
        private int defaultCharsetCollation;
        private Map<Integer, Integer> charsetCollations;

        public void setDefaultCharsetCollation(int defaultCharsetCollation) {
            this.defaultCharsetCollation = defaultCharsetCollation;
        }

        public int getDefaultCharsetCollation() {
            return defaultCharsetCollation;
        }

        public void setCharsetCollations(Map<Integer, Integer> charsetCollations) {
            this.charsetCollations = charsetCollations;
        }

        public Map<Integer, Integer> getCharsetCollations() {
            return charsetCollations;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(defaultCharsetCollation);
            sb.append(", charsetCollations=") .append(charsetCollations == null ? "null" : "");
            appendMap(sb, charsetCollations);
            return sb.toString();
        }
    }
}
