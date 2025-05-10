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

import org.apache.doris.thrift.TEncodingType;

import java.util.HashMap;
import java.util.Map;

public class EncodingInfo {
    private static final Map<String, Integer> encodingMap = new HashMap<>();
    private static final Map<Integer, String> encodingNumberMap = new HashMap<>();

    static {
        encodingMap.put("DEFAULT", TEncodingType.DEFAULT_ENCODING.getValue());
        encodingNumberMap.put(TEncodingType.DEFAULT_ENCODING.getValue(), "DEFAULT");

        encodingMap.put("PLAIN", TEncodingType.PLAIN_ENCODING.getValue());
        encodingNumberMap.put(TEncodingType.PLAIN_ENCODING.getValue(), "PLAIN");

        encodingMap.put("PREFIX", TEncodingType.PREFIX_ENCODING.getValue());
        encodingNumberMap.put(TEncodingType.PREFIX_ENCODING.getValue(), "PREFIX");

        encodingMap.put("RLE", TEncodingType.RLE.getValue());
        encodingNumberMap.put(TEncodingType.RLE.getValue(), "RLE");

        encodingMap.put("DICT", TEncodingType.DICT_ENCODING.getValue());
        encodingNumberMap.put(TEncodingType.DICT_ENCODING.getValue(), "DICT");

        encodingMap.put("BIT_SHUFFLE", TEncodingType.BIT_SHUFFLE.getValue());
        encodingNumberMap.put(TEncodingType.BIT_SHUFFLE.getValue(), "BIT_SHUFFLE");
        encodingMap.put("FOR", TEncodingType.FOR_ENCODING.getValue());
        encodingNumberMap.put(TEncodingType.FOR_ENCODING.getValue(), "FOR");
    }

    public static String getEncodingString(int number) {
        return encodingNumberMap.get(number);
    }

    public static boolean isDefaultEncoding(String encoding) {
        return "DEFAULT".equalsIgnoreCase(encoding);
    }

    public static Integer getEncodingNumber(String encoding) {
        return encoding == null ? null : encodingMap.get(encoding.toUpperCase());
    }
}
