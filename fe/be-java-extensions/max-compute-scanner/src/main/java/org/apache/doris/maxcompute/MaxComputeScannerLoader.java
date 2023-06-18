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

package org.apache.doris.maxcompute;

import org.apache.doris.common.classloader.ChildFirstClassLoader;
import org.apache.doris.common.jni.ScannerLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * The maxcompute scan loader
 */
public class MaxComputeScannerLoader implements ScannerLoader {

    private static final ChildFirstClassLoader classLoader;

    static {
        String basePath = System.getenv("DORIS_HOME");
        URL[] maxComputeAssemblyJar;
        try {
            File maxComputeAssemblyFile = new File(basePath + "/lib/max-compute-jar-with-dependencies.jar");
            maxComputeAssemblyJar = new URL[]{maxComputeAssemblyFile.toURI().toURL()};
        } catch (MalformedURLException e) {
            e.printStackTrace();
            throw new RuntimeException("MaxCompute scanner loader error", e);
        }
        classLoader = new ChildFirstClassLoader(maxComputeAssemblyJar, ClassLoader.getSystemClassLoader());
    }

    @Override
    public Class getScannerClass() throws ClassNotFoundException {
        try {
            return classLoader.loadClass("org.apache.doris.maxcompute.MaxComputeJniScanner");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
