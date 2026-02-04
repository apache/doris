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

package org.apache.doris.common.jni.utils;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility for BE JNI callers to use Java PathMatcher glob semantics.
 */
public final class PathMatcherUtil {
    private static final String CACHE_SIZE_KEY = "doris.pathmatcher.cache.max";
    private static final int DEFAULT_CACHE_MAX = 2048;
    private static final int CACHE_MAX = initCacheMax();

    private static final Map<String, PathMatcher> CACHE = Collections.synchronizedMap(
            new LinkedHashMap<String, PathMatcher>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, PathMatcher> eldest) {
                    return size() > CACHE_MAX;
                }
            });

    private PathMatcherUtil() {
    }

    public static boolean matches(String pattern, String path) {
        if (pattern == null || path == null) {
            return false;
        }
        try {
            PathMatcher matcher = CACHE.get(pattern);
            if (matcher == null) {
                matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
                CACHE.put(pattern, matcher);
            }
            return matcher.matches(Paths.get(path));
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static int initCacheMax() {
        String prop = System.getProperty(CACHE_SIZE_KEY);
        if (prop == null || prop.isEmpty()) {
            return DEFAULT_CACHE_MAX;
        }
        try {
            int value = Integer.parseInt(prop);
            return value > 0 ? value : DEFAULT_CACHE_MAX;
        } catch (NumberFormatException e) {
            return DEFAULT_CACHE_MAX;
        }
    }
}
