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

package org.apache.doris.statistics.sampling;

import org.apache.doris.analysis.FixedBlockSampleSpec;

import java.util.List;

public record TabletSampleSpec(SampledScanClause scanClause, SamplingSemantics semantics) {
    public sealed interface SampledScanClause permits FullScanClause, LegacyTabletScanClause, FixedBlockScanClause {
    }

    public record FullScanClause() implements SampledScanClause {
    }

    public record LegacyTabletScanClause(List<Long> tablets) implements SampledScanClause {
        public LegacyTabletScanClause {
            tablets = List.copyOf(tablets);
        }
    }

    public record FixedBlockScanClause(FixedBlockSampleSpec blocks) implements SampledScanClause {
    }

    public sealed interface SamplingSemantics permits FullScanSemantics, LegacySamplingSemantics,
            EqualProbabilitySamplingSemantics {
    }

    public record FullScanSemantics() implements SamplingSemantics {
    }

    public record LegacySamplingSemantics(boolean linear, double scaleFactor, long limit) implements SamplingSemantics {
    }

    public record EqualProbabilitySamplingSemantics(double probability) implements SamplingSemantics {
    }

    public static TabletSampleSpec fullScan() {
        return new TabletSampleSpec(new FullScanClause(), new FullScanSemantics());
    }
}
