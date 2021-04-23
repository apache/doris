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

package org.apache.doris.flink.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

public class IOUtils {
    public static String propsToString(Properties props) throws IllegalArgumentException {
        StringWriter sw = new StringWriter();
        if (props != null) {
            try {
                props.store(sw, "");
            } catch (IOException ex) {
                throw new IllegalArgumentException("Cannot parse props to String.", ex);
            }
        }
        return sw.toString();
    }

    public static Properties propsFromString(String source) throws IllegalArgumentException {
        Properties copy = new Properties();
        if (source != null) {
            try {
                copy.load(new StringReader(source));
            } catch (IOException ex) {
                throw new IllegalArgumentException("Cannot parse props from String.", ex);
            }
        }
        return copy;
    }
}
