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

package org.apache.doris.load;

import org.apache.doris.common.FeConstants;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.Set;

public final class LoadJobInfoFormatter {
    private static final Set<String> TASK_DETAIL_FIELDS = ImmutableSet.of(
            "ScannedRows",
            "LoadBytes",
            "FileNumber",
            "FileSize",
            "FilteredRows",
            "Unfinished backends",
            "All backends");

    private LoadJobInfoFormatter() {
    }

    public static String buildEtlInfo(String counters, String etlStartTime, String etlFinishTime) {
        JsonObject etlInfo = new JsonObject();
        addDelimitedProperties(etlInfo, counters, "=");
        addPropertyIfPresent(etlInfo, "ETL_START_TIME", etlStartTime);
        addPropertyIfPresent(etlInfo, "ETL_FINISH_TIME", etlFinishTime);
        return etlInfo.toString();
    }

    public static String buildTaskInfo(String taskContext, String jobDetails) {
        JsonObject taskInfo = new JsonObject();
        addDelimitedProperties(taskInfo, taskContext, ":");
        if (!isEmptyValue(jobDetails)) {
            JsonObject details = JsonParser.parseString(jobDetails).getAsJsonObject();
            for (Map.Entry<String, JsonElement> entry : details.entrySet()) {
                if (TASK_DETAIL_FIELDS.contains(entry.getKey())) {
                    taskInfo.add(entry.getKey(), entry.getValue());
                }
            }
        }
        return taskInfo.toString();
    }

    public static String buildErrorDetail(String url, String errorTablets, String errorMessage) {
        JsonObject errorDetail = new JsonObject();
        errorDetail.addProperty("URL", emptyIfAbsent(url));
        errorDetail.addProperty("ERROR_TABLETS", emptyIfAbsent(errorTablets));
        errorDetail.addProperty("ERROR_MSG", emptyIfAbsent(errorMessage));
        return errorDetail.toString();
    }

    private static void addDelimitedProperties(JsonObject target, String value, String delimiter) {
        if (isEmptyValue(value)) {
            return;
        }
        for (String property : value.split(";")) {
            String[] keyValue = property.trim().split(delimiter, 2);
            target.addProperty(keyValue[0].trim(), keyValue[1].trim());
        }
    }

    private static void addPropertyIfPresent(JsonObject target, String key, String value) {
        if (!isEmptyValue(value)) {
            target.addProperty(key, value);
        }
    }

    private static String emptyIfAbsent(String value) {
        return isEmptyValue(value) ? "" : value;
    }

    private static boolean isEmptyValue(String value) {
        return Strings.isNullOrEmpty(value) || FeConstants.null_string.equals(value);
    }
}
