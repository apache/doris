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

package org.apache.doris.resource;

public final class BackendSelection {
    public enum Mode {
        PREFER,
        REQUIRE,
        DEFAULT
    }

    public static final class SelectionHint {
        private final String preferredKey;
        private final Mode mode;
        private final String reason;

        public SelectionHint(String preferredKey, Mode mode, String reason) {
            this.preferredKey = preferredKey == null ? "" : preferredKey;
            this.mode = mode == null ? Mode.DEFAULT : mode;
            this.reason = reason == null ? "" : reason;
        }

        public static SelectionHint noSelection() {
            return new SelectionHint("", Mode.DEFAULT, "no_preferred");
        }

        public String getPreferredKey() {
            return preferredKey;
        }

        public Mode getMode() {
            return mode;
        }

        public String getReason() {
            return reason;
        }
    }

    private BackendSelection() {
    }
}
