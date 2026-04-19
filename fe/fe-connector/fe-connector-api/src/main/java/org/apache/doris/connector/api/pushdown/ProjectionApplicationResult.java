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

package org.apache.doris.connector.api.pushdown;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result of applying a projection to a table handle.
 *
 * @param <T> the table handle type
 */
public final class ProjectionApplicationResult<T> {

    private final T handle;
    private final List<ConnectorExpression> projections;
    private final List<ConnectorColumnAssignment> assignments;

    public ProjectionApplicationResult(T handle,
            List<ConnectorExpression> projections,
            List<ConnectorColumnAssignment> assignments) {
        this.handle = Objects.requireNonNull(handle, "handle");
        this.projections = projections == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(projections);
        this.assignments = assignments == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(assignments);
    }

    public T getHandle() {
        return handle;
    }

    public List<ConnectorExpression> getProjections() {
        return projections;
    }

    public List<ConnectorColumnAssignment> getAssignments() {
        return assignments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProjectionApplicationResult)) {
            return false;
        }
        ProjectionApplicationResult<?> that =
                (ProjectionApplicationResult<?>) o;
        return handle.equals(that.handle)
                && projections.equals(that.projections)
                && assignments.equals(that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handle, projections, assignments);
    }

    @Override
    public String toString() {
        return "ProjectionApplicationResult{handle=" + handle
                + ", projections=" + projections.size()
                + ", assignments=" + assignments.size() + "}";
    }
}
