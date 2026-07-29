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

package org.apache.doris.paimon;

import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonUtils {
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();
    private static final Base64.Decoder STD_DECODER = Base64.getDecoder();

    public static List<String> getFieldNames(RowType rowType) {
        return rowType.getFields().stream()
                .map(DataField::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    public static <T> T deserialize(String encodedStr) {
        try {
            byte[] decoded;
            try {
                decoded = URL_DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (IllegalArgumentException e) {
                // Fallback to standard Base64 for splits encoded by native Paimon serialization.
                decoded = STD_DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            return InstantiationUtil.deserializeObject(decoded, PaimonUtils.class.getClassLoader());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static Table getTableFromParams(Map<String, String> params) {
        if (params.containsKey("serialized_table")) {
            return deserialize(params.get("serialized_table"));
        }

        // 2. 解析基础 ID 信息
        String dbName = params.get("db_name");
        String tblName = params.get("table_name");
        long ctlId = parseLongOrDefault(params.get("ctl_id"), -1L);
        long dbId = parseLongOrDefault(params.get("db_id"), -1L);
        long tblId = parseLongOrDefault(params.get("tbl_id"), -1L);
        long lastUpdateTime = parseLongOrDefault(params.get("last_update_time"), -1L);

        // 3. 提取 paimon. 和 hadoop. 配置项
        Map<String, String> paimonOptionParams = new HashMap<>();
        Map<String, String> hadoopOptionParams = new HashMap<>();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("paimon.")) {
                paimonOptionParams.put(key.substring("paimon.".length()), entry.getValue());
            } else if (key.startsWith("hadoop.")) {
                hadoopOptionParams.put(key.substring("hadoop.".length()), entry.getValue());
            }
        }

        // 4. 从缓存获取 Table
        PaimonTableCache.PaimonTableCacheKey key = new PaimonTableCache.PaimonTableCacheKey(
                ctlId, dbId, tblId, paimonOptionParams, hadoopOptionParams, dbName, tblName);

        PaimonTableCache.TableExt tableExt = PaimonTableCache.getTable(key);

        // 5. 检查元数据是否过期
        if (tableExt.getCreateTime() < lastUpdateTime) {
            // 如果缓存表的创建时间早于 FE 传递的 lastUpdateTime，说明表结构发生了变更，需刷新缓存
            PaimonTableCache.invalidateTableCache(key);
            tableExt = PaimonTableCache.getTable(key);
        }

        return tableExt.getTable();
    }

    private static long parseLongOrDefault(String v, long defaultValue) {
        if (v == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
