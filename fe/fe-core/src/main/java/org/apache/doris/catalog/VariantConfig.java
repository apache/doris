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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TVariantConfig;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VariantConfig implements Writable {
    public static double DEFAULT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN = 1.0;

    public static long DEFAULT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN = 1024;

    @SerializedName("enableDecimalType")
    private boolean enableDecimalType;

    @SerializedName("ratioOfDefaultsAsSparseColumn")
    private double ratioOfDefaultsAsSparseColumn;

    @SerializedName("thresholdRowsToEstimateSparseColumn")
    private long thresholdRowsToEstimateSparseColumn;

    public VariantConfig(boolean enableDecimalType, double ratioOfDefaultsAsSparseColumn,
                        long thresholdRowsToEstimateSparseColumn) {
        this.enableDecimalType = enableDecimalType;
        this.ratioOfDefaultsAsSparseColumn = ratioOfDefaultsAsSparseColumn;
        this.thresholdRowsToEstimateSparseColumn = thresholdRowsToEstimateSparseColumn;
    }

    public VariantConfig(VariantConfig config) {
        this(config.enableDecimalType, config.ratioOfDefaultsAsSparseColumn,
                config.thresholdRowsToEstimateSparseColumn);
    }

    public VariantConfig() {
        this(false, DEFAULT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN,
                DEFAULT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN);
    }

    public boolean isEnableDecimalType() {
        return enableDecimalType;
    }

    public void setEnableDecimalType(boolean enableDecimalType) {
        this.enableDecimalType = enableDecimalType;
    }

    public double getRatioOfDefaultsAsSparseColumn() {
        return ratioOfDefaultsAsSparseColumn;
    }

    public void setRatioOfDefaultsAsSparseColumn(double ratioOfDefaultsAsSparseColumn) {
        this.ratioOfDefaultsAsSparseColumn = ratioOfDefaultsAsSparseColumn;
    }

    public long getThresholdRowsToEstimateSparseColumn() {
        return thresholdRowsToEstimateSparseColumn;
    }

    public void setThresholdRowsToEstimateSparseColumn(long thresholdRowsToEstimateSparseColumn) {
        this.thresholdRowsToEstimateSparseColumn = thresholdRowsToEstimateSparseColumn;
    }

    public void mergeFromProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_DECIMAL_TYPE)) {
            enableDecimalType = Boolean.parseBoolean(properties.get(
                    PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_DECIMAL_TYPE));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_VARIANT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN)) {
            ratioOfDefaultsAsSparseColumn = Double.parseDouble(properties.get(
                    PropertyAnalyzer.PROPERTIES_VARIANT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN));
        }
        if (properties.containsKey(PropertyAnalyzer.VARIANT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN)) {
            thresholdRowsToEstimateSparseColumn = Long.parseLong(properties.get(
                    PropertyAnalyzer.VARIANT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN));

        }
    }

    public TVariantConfig toThrift() {
        TVariantConfig tVariantConfig = new TVariantConfig();
        tVariantConfig.setEnableDecimalType(enableDecimalType);
        tVariantConfig.setRatioOfDefaultsAsSparseColumn(ratioOfDefaultsAsSparseColumn);
        tVariantConfig.setThresholdRowsToEstimateSparseColumn(thresholdRowsToEstimateSparseColumn);
        return tVariantConfig;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_DECIMAL_TYPE, String.valueOf(enableDecimalType));
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN,
                String.valueOf(ratioOfDefaultsAsSparseColumn));
        properties.put(PropertyAnalyzer.VARIANT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN,
                String.valueOf(thresholdRowsToEstimateSparseColumn));
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof VariantConfig)) {
            return false;
        }

        VariantConfig other = (VariantConfig) obj;
        if (this.enableDecimalType != other.enableDecimalType) {
            return false;
        }
        if (this.ratioOfDefaultsAsSparseColumn != other.ratioOfDefaultsAsSparseColumn) {
            return false;
        }
        return this.thresholdRowsToEstimateSparseColumn == other.thresholdRowsToEstimateSparseColumn;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static VariantConfig read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), VariantConfig.class);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public void appendToShowCreateTable(StringBuilder sb) {
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_DECIMAL_TYPE)
                .append("\" = \"")
                .append(enableDecimalType)
                .append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VARIANT_RATIO_OF_DEFAULTS_AS_SPARSE_COLUMN)
                .append("\" = \"")
                .append(ratioOfDefaultsAsSparseColumn)
                .append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.VARIANT_THRESHOLD_ROWS_TO_ESTIMATE_SPARSE_COLUMN)
                .append("\" = \"")
                .append(thresholdRowsToEstimateSparseColumn)
                .append("\"");
    }

    public static VariantConfig fromProperties(Map<String, String> properties) {
        VariantConfig variantConfig = new VariantConfig();
        variantConfig.mergeFromProperties(properties);
        return variantConfig;
    }
}
