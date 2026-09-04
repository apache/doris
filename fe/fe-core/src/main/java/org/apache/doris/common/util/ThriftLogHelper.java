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

package org.apache.doris.common.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.util.Map;

/** Creates log-only copies of Thrift requests with credential fields masked. */
public final class ThriftLogHelper {
    private static final String MASKED_CREDENTIAL = "***MASKED***";

    private ThriftLogHelper() {
    }

    // auth_code is omitted because its numeric field cannot hold the string mask.
    public static <T extends TBase<T, F>, F extends TFieldIdEnum> T requestForLog(T request) {
        T requestForLog = request.deepCopy();
        for (Map.Entry<F, FieldMetaData> entry : fieldMetadata(request).entrySet()) {
            F field = entry.getKey();
            if (!requestForLog.isSet(field)) {
                continue;
            }

            String fieldName = entry.getValue().fieldName;
            if ("auth_code".equals(fieldName)) {
                requestForLog.setFieldValue(field, null);
            } else if ("passwd".equals(fieldName) || "token".equals(fieldName)
                    || "auth_code_uuid".equals(fieldName)) {
                requestForLog.setFieldValue(field, MASKED_CREDENTIAL);
            }
        }
        return requestForLog;
    }

    @SuppressWarnings("unchecked")
    private static <T extends TBase<T, F>, F extends TFieldIdEnum> Map<F, FieldMetaData> fieldMetadata(T request) {
        return (Map<F, FieldMetaData>) FieldMetaData.getStructMetaDataMap(request.getClass());
    }
}
