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

package org.apache.doris.nereids.metrics;

import java.util.Objects;

/**
 * event enhancer
 */
public abstract class EventEnhancer {
    private final Class<? extends Event> targetClass;

    public EventEnhancer(Class<? extends Event> targetClass) {
        this.targetClass = targetClass;
    }

    public Class<? extends Event> getTargetClass() {
        return targetClass;
    }

    public abstract void enhance(Event e);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventEnhancer enhancer = (EventEnhancer) o;
        return Objects.equals(targetClass, enhancer.targetClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetClass);
    }
}
