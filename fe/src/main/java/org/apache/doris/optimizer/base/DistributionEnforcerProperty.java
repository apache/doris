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

package org.apache.doris.optimizer.base;

import java.util.Objects;

public class DistributionEnforcerProperty extends EnforcerProperty {
    public static final DistributionEnforcerProperty ANY =
            new DistributionEnforcerProperty(OptDistributionSpec.createAnyDistributionSpec());
    private final OptDistributionSpec spec;

    public DistributionEnforcerProperty(OptDistributionSpec spec) {
        this.spec = spec;
    }

    @Override
    public OptDistributionSpec getPropertySpec() {
        return spec;
    }

    @Override
    public boolean isSatisfy(OptPropertySpec spec) {
        return spec.isSatisfy(spec);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributionEnforcerProperty that = (DistributionEnforcerProperty) o;
        return Objects.equals(spec, that.spec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spec);
    }
}
