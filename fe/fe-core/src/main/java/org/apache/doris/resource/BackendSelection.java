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

import com.google.gson.annotations.SerializedName;

import java.util.List;

public final class BackendSelection {
    public enum Mode {
        PREFER,
        REQUIRE,
        DEFAULT
    }

    /** Outcome of query backend selection for one scan candidate set. */
    public enum QuerySelectionResult {
        DISABLED,
        PREFERRED_HIT,
        FALLBACK_PREFERRED_UNAVAILABLE
    }

    /** Provider partition used by the kernel to enforce required backend selection. */
    public static final class CandidateSelection<T> {
        private final List<T> preferredCandidates;
        private final List<T> fallbackCandidates;

        public CandidateSelection(List<T> preferredCandidates, List<T> fallbackCandidates) {
            this.preferredCandidates = preferredCandidates;
            this.fallbackCandidates = fallbackCandidates;
        }

        public List<T> getPreferredCandidates() {
            return preferredCandidates;
        }

        public List<T> getFallbackCandidates() {
            return fallbackCandidates;
        }
    }

    public static final class SelectionHint {
        @SerializedName("k")
        private final String preferredKey;
        @SerializedName("m")
        private final Mode mode;
        @SerializedName("r")
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
            return preferredKey == null ? "" : preferredKey;
        }

        public Mode getMode() {
            return mode == null ? Mode.DEFAULT : mode;
        }

        public String getReason() {
            return reason == null ? "" : reason;
        }
    }

    private BackendSelection() {
    }
}
