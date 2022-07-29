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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

import java.io.StringWriter;

public class Separator implements ParseNode {
    private static final String HEX_STRING = "0123456789ABCDEF";

    private final String oriSeparator;
    private String separator;

    public Separator(String separator) {
        this.oriSeparator = separator;
        this.separator = null;
    }

    public String getOriSeparator() {
        return oriSeparator;
    }

    public String getSeparator() {
        return separator;
    }

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }

    public void analyze() throws AnalysisException {
        analyze(null);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        this.separator = convertSeparator(oriSeparator);
    }

    public static String convertSeparator(String originStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(originStr)) {
            throw new AnalysisException("Column separator is null or empty");
        }

        if (originStr.toUpperCase().startsWith("\\X")) {
            // convert \x01\x02\x0a to 01020a
            String hexStr = originStr.replaceAll("(?i)\\\\X", "");
            // check hex str
            if (hexStr.isEmpty()) {
                throw new AnalysisException("Hex str is empty");
            }
            for (char hexChar : hexStr.toUpperCase().toCharArray()) {
                if (HEX_STRING.indexOf(hexChar) == -1) {
                    throw new AnalysisException("Hex str format error");
                }
            }
            if (hexStr.length() % 2 != 0) {
                throw new AnalysisException("Hex str length error");
            }

            // transform to separator
            StringWriter writer = new StringWriter();
            for (byte b : hexStrToBytes(hexStr)) {
                writer.append((char) b);
            }
            return writer.toString();
        } else {
            return unescape(originStr);
        }
    }

    // unescape some invisible string literal to char, such as "/t" to char '/t'
    private static String unescape(String orig) {
        StringBuilder sb = new StringBuilder();
        boolean hadSlash = false;
        for (int i = 0; i < orig.length(); i++) {
            char ch = orig.charAt(i);
            if (hadSlash) {
                hadSlash = false;
                switch (ch) {
                    case '\\':
                        sb.append('\\');
                        break;
                    case '\'':
                        sb.append('\'');
                        break;
                    case '\"':
                        sb.append('"');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    default:
                        sb.append('\\').append(ch);
                        break;
                }
                continue;
            } else if (ch == '\\') {
                if (i == orig.length() - 1) {
                    sb.append(ch);
                } else {
                    hadSlash = true;
                }
                continue;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("'").append(oriSeparator).append("'");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
