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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Reference to Apache Flink ChildFirstClassLoader
 */
public class ChildFirstClassLoader extends URLClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    private static final Logger LOG = Logger.getLogger(ChildFirstClassLoader.class);

    private ParentClassLoader parent;

    private final String[] alwaysParentFirstPatterns = new String[]{
        "org.apache.doris.common.jni.utils.JNINativeMethod"
    };

    public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, null);
        this.parent = new ParentClassLoader(parent);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (Arrays.stream(alwaysParentFirstPatterns).anyMatch(c -> c.startsWith(name))) {
            return parent.loadClass(name, resolve);
        }
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException cnf) {
            LOG.error("loadClass error", cnf);
        }
        return parent.loadClass(name, resolve);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        ArrayList<URL> urls = Collections.list(super.getResources(name));
        urls.addAll(Collections.list(parent.getResources(name)));
        return Collections.enumeration(urls);
    }

    @Override
    public URL getResource(String name) {
        URL url = super.getResource(name);
        if (url != null) {
            return url;
        } else {
            return parent.getResource(name);
        }
    }
}
