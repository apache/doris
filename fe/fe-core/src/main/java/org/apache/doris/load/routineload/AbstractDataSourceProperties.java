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

package org.apache.doris.load.routineload;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * Abstract class for data source properties
 * All routine load data source properties should extend this class
 */
@Data
public abstract class AbstractDataSourceProperties {

    /**
     * Original data source properties
     * we can use this map to get all original properties
     * and this is only a temporary parameter and will be of no use after convert ends
     */
    @Getter
    @SerializedName(value = "originalDataSourceProperties")
    protected Map<String, String> originalDataSourceProperties;

    @SerializedName(value = "type")
    private String dataSourceType;

    /**
     * Is it an ALTER operation
     */
    private boolean isAlter = false;

    @SerializedName(value = "timezone")
    protected String timezone;


    public AbstractDataSourceProperties(Map<String, String> dataSourceProperties, boolean multiTable) {
        this.originalDataSourceProperties = dataSourceProperties;
        this.multiTable = multiTable;
    }

    public AbstractDataSourceProperties(Map<String, String> originalDataSourceProperties) {
        this.originalDataSourceProperties = originalDataSourceProperties;
    }

    protected abstract String getDataSourceType();

    protected abstract List<String> getRequiredProperties() throws UserException;

    /**
     * Whether the data source is multi load
     * default is false
     */
    protected boolean multiTable = false;

    /**
     * Check required properties
     * we can check for optional mutex parameters, and whether the concrete type is null in the future
     *
     * @throws UserException
     */
    protected void checkRequiredProperties() throws UserException {
        if (isAlter) {
            return;
        }
        List<String> requiredProperties = getRequiredProperties();
        for (String requiredProperty : requiredProperties) {
            if (!originalDataSourceProperties.containsKey(requiredProperty)
                    && null != originalDataSourceProperties.get(requiredProperty)) {
                throw new IllegalArgumentException("Required property " + requiredProperty + " is missing");
            }
        }
    }

    public void analyze() throws UserException {
        if (isAlter && MapUtils.isEmpty(originalDataSourceProperties)) {
            throw new AnalysisException("No data source properties");
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(timezone), "timezone must be set before analyzing");
        checkRequiredProperties();
        this.dataSourceType = getDataSourceType();
        this.convertAndCheckDataSourceProperties();
    }

    /**
     * Convert and check data source properties
     * This method should be implemented by sub class
     * It will be called in analyze method
     * It will convert data source properties to correct type
     *
     * @throws UserException if any error occurs
     */
    public abstract void convertAndCheckDataSourceProperties() throws UserException;
}
