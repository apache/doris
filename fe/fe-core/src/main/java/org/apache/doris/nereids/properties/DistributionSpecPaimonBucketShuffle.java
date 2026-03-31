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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Paimon bucket shuffle distribution spec.
 */
public class DistributionSpecPaimonBucketShuffle extends DistributionSpec {
    private final List<Expression> partitionExpressions;

    public DistributionSpecPaimonBucketShuffle(List<Expression> partitionExpressions) {
        this.partitionExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(partitionExpressions, "partitionExpressions should not null"));
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        if (other instanceof DistributionSpecAny) {
            return true;
        }
        return this.equals(other);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("DistributionSpecPaimonBucketShuffle", "exprs", partitionExpressions);
    }

    @Override
    public String shapeInfo() {
        return "DistributionSpecPaimonBucketShuffle";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionSpecPaimonBucketShuffle that = (DistributionSpecPaimonBucketShuffle) o;
        return partitionExpressions.equals(that.partitionExpressions);
    }

    @Override
    public int hashCode() {
        return partitionExpressions.hashCode();
    }
}
