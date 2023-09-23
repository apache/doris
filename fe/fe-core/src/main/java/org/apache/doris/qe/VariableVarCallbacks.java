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

import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Callback after setting the session variable
 */
public class VariableVarCallbacks {

    public static final Map<String, VariableVarCallbackI> callbacks = Maps.newHashMap();

    static {
        SessionContextCallback sessionContextCallback = new SessionContextCallback();
        callbacks.put(SessionVariable.SESSION_CONTEXT, sessionContextCallback);
    }

    public static Boolean hasCallback(String varName) {
        return callbacks.containsKey(varName);
    }

    public static void call(String varName, String value) throws DdlException {
        if (hasCallback(varName)) {
            callbacks.get(varName).call(value);
        }
    }

    // Converter to convert runtime filter type variable
    public static class SessionContextCallback implements VariableVarCallbackI {
        public void call(String value) throws DdlException {
            if (Strings.isNullOrEmpty(value)) {
                return;
            }
            /**
             * The sessionContext is as follows:
             * "k1:v1;k2:v2;..."
             * Here we want to get value with key named "trace_id".
             */
            String[] parts = value.split(";");
            for (String part : parts) {
                String[] innerParts = part.split(":");
                if (innerParts.length != 2) {
                    continue;
                }
                if (innerParts[0].equals("trace_id")) {
                    ConnectContext.get().setTraceId(innerParts[1]);
                    break;
                }
            }
        }
    }
}
