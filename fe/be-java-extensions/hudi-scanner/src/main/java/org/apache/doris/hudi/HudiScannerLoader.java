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

package org.apache.doris.hudi;

import org.apache.doris.common.classloader.ChildFirstClassLoader;
import org.apache.doris.common.jni.ScannerLoader;

import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * The hudi scan loader
 */
public class HudiScannerLoader implements ScannerLoader {
    private static final Logger LOG = Logger.getLogger(ScannerLoader.class);

    private static final ChildFirstClassLoader classLoader;

    static {
        String basePath = System.getenv("DORIS_HOME");
        URL[] hudiAssemblyJar;
        try {
            File hudiAssemblyFile =
                    new File(basePath + "/lib/java_extensions/hudi-scanner-jar-with-dependencies.jar");
            hudiAssemblyJar = new URL[] {hudiAssemblyFile.toURI().toURL()};
        } catch (MalformedURLException e) {
            e.printStackTrace();
            throw new RuntimeException("Hudi scanner loader error", e);
        }
        classLoader = new ChildFirstClassLoader(hudiAssemblyJar, ClassLoader.getSystemClassLoader());
    }

    @Override
    public Class getScannerClass() throws ClassNotFoundException {
        try {
            Class hudi = classLoader.loadClass("org.apache.doris.hudi.HudiJniScanner");
            LOG.info("Get hudi scanner from scanner loader, path="
                    + ChildFirstClassLoader.getClassLoadPath(hudi).getPath());
            return hudi;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
