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

public class EnforceOrderProperty extends EnforceProperty {
    private OptOrderSpec spec;

    public EnforceOrderProperty(OptOrderSpec spec) {
    }

    public static EnforceOrderProperty createEmpty() {
        return new EnforceOrderProperty(OptOrderSpec.createEmpty());
    }

    @Override
    public OptOrderSpec getPropertySpec() { return spec; }

    // check if this property contains the given one
    public boolean contains(OptOrderSpec otherSpec) {
        return spec.contains(otherSpec);
    }

    // check if this property is a subset of the given OrderSpec
    public boolean isSubset(OptOrderSpec optOrderSpec) {
        return optOrderSpec.contains(spec);
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EnforceOrderProperty)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        EnforceOrderProperty rhs = (EnforceOrderProperty) obj;
        return spec.equals(rhs.spec);
    }
}
