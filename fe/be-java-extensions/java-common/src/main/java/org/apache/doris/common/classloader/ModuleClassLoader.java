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

package org.apache.doris.common.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;

public class ModuleClassLoader extends URLClassLoader {
    public static URL getClassLoadPath(Class<?> loadClass) {
        ProtectionDomain protectionDomain = loadClass.getProtectionDomain();
        CodeSource codeSource = protectionDomain.getCodeSource();
        if (codeSource != null) {
            return codeSource.getLocation();
        } else {
            return null;
        }
    }

    private final ClassLoader defaultClassLoader;

    public ModuleClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        defaultClassLoader = parent;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException {
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException e) {
            return defaultClassLoader.loadClass(name);
        }
    }
}
