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

package org.apache.doris.common.profile;

public class ProfileSpan implements AutoCloseable {

    public interface TimingOperation {
        void execute(String id, String step);
    }

    public static final ProfileSpan EMPTY_PROFILE_SPAN = new ProfileSpan("", "",
            (id, step) -> {},
            (id, step) -> {}
    );

    private final String id;
    private final String step;
    private final TimingOperation finishOperation;

    public ProfileSpan(String id, String step,
            TimingOperation startOperation,
            TimingOperation finishOperation) {
        this.id = id;
        this.step = step;
        this.finishOperation = finishOperation;
        startOperation.execute(id, step);
    }

    @Override
    public void close() {
        finishOperation.execute(id, step);
    }

    public static ProfileSpan create(SummaryProfile sp, String id, String step) {
        if (sp != null) {
            return new ProfileSpan(id, step, (id1, step1) -> sp.getNodeTimer(id1).startStep(step1),
                    (id2, step2) -> sp.getNodeTimer(id2).stopStep(step2));
        } else {
            return EMPTY_PROFILE_SPAN;
        }
    }
}
