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

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.PluginFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result summary of one {@code loadAll} invocation.
 */
public final class LoadReport<F extends PluginFactory> {

    private final List<PluginHandle<F>> successes;
    private final List<LoadFailure> failures;
    private final int rootsScanned;
    private final int dirsScanned;

    public LoadReport(List<PluginHandle<F>> successes, List<LoadFailure> failures, int rootsScanned, int dirsScanned) {
        this.successes = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(successes, "successes")));
        this.failures = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(failures, "failures")));
        this.rootsScanned = rootsScanned;
        this.dirsScanned = dirsScanned;
    }

    public List<PluginHandle<F>> getSuccesses() {
        return successes;
    }

    public List<LoadFailure> getFailures() {
        return failures;
    }

    public int getRootsScanned() {
        return rootsScanned;
    }

    public int getDirsScanned() {
        return dirsScanned;
    }
}
