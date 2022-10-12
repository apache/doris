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

package org.apache.doris.regression.util

class LoggerUtils {
    static Tuple2<Integer, String> getErrorInfo(Throwable t, File file) {
        if (file.name.endsWith(".groovy")) {
            int lineNumber = -1
            for (def st : t.getStackTrace()) {
                if (Objects.equals(st.fileName, file.name)) {
                    lineNumber = st.getLineNumber()
                    break
                }
            }
            if (lineNumber == -1) {
                return new Tuple2<Integer, String>(null, null)
            }

            List<String> lines = file.text.split("\n").toList()
            String errorPrefixText = lines.subList(Math.max(0, lineNumber - 10), lineNumber).join("\n")
            String errorSuffixText = lines.subList(lineNumber, Math.min(lines.size(), lineNumber + 10)).join("\n")
            String errorText = "${errorPrefixText}\n^^^^^^^^^^^^^^^^^^^^^^^^^^ERROR LINE^^^^^^^^^^^^^^^^^^^^^^^^^^\n${errorSuffixText}".toString()
            return new Tuple2<Integer, String>(lineNumber, errorText)
        } else {
            return new Tuple2<Integer, String>(null, null)
        }
    }
}
