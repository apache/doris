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

import org.apache.doris.common.Config;
import org.apache.doris.common.LogUtils;
import org.apache.doris.common.Version;

import com.google.common.annotations.VisibleForTesting;

public class JdkUtils {

    /*
     * Doris will check if the compiled and running versions of Java are compatible.
     * The principle the java version at runtime should higher than or equal to the
     * java version at compile time
     */
    @VisibleForTesting
    public static boolean checkJavaVersion() {
        if (!Config.check_java_version) {
            return true;
        }

        String javaCompileVersionStr = getJavaVersionFromFullVersion(Version.DORIS_JAVA_COMPILE_VERSION);
        String javaRuntimeVersionStr = System.getProperty("java.version");

        int compileVersion = JdkUtils.getJavaVersionAsInteger(javaCompileVersionStr);
        int runtimeVersion = JdkUtils.getJavaVersionAsInteger(javaRuntimeVersionStr);

        if (runtimeVersion < compileVersion) {
            LogUtils.stdout("The runtime java version " + javaRuntimeVersionStr + " is less than "
                    + "compile version " + javaCompileVersionStr);
            return false;
        }

        return true;
    }

    /*
     * Input: openjdk full 'version "13.0.1+9"', 'java full version "1.8.0_131-b11"'
     * Output: '13.0.1+9', '1.8.0_131-b11'
     */
    public static String getJavaVersionFromFullVersion(String fullVersionStr) {
        int begin = fullVersionStr.indexOf("\"");
        int end = fullVersionStr.lastIndexOf("\"");
        String versionStr = fullVersionStr.substring(begin + 1, end);
        return versionStr;
    }

    /*
     * Input: '13.0.1+9', '1.8.0_131-b11'
     * Output: 13, 8
     */
    public static int getJavaVersionAsInteger(String javaVersionStr) {
        String[] parts = javaVersionStr.split("\\.|\\+");
        if (parts[0].equals("1")) {
            return Integer.valueOf(parts[1]);
        } else {
            return Integer.valueOf(parts[0]);
        }
    }
}
