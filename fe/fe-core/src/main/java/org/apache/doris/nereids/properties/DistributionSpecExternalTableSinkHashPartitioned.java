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

import org.apache.doris.nereids.trees.expressions.ExprId;

import java.util.List;
import java.util.Objects;

/** Distribution spec for external table sink hash writer routing. */
public class DistributionSpecExternalTableSinkHashPartitioned extends DistributionSpec {

    /** External table sink hash distribution mode. */
    public enum ExternalSinkHashMode {
        /** Use scale writer to rebalance writer load. */
        SCALE_WRITER,
        /** Use deterministic hash routing without writer rebalance. */
        STRICT_HASH
    }

    /** Paimon fixed bucket route descriptor. */
    public static class PaimonFixedBucketRouteInfo {
        /** Paimon fixed bucket function type. */
        public enum BucketFunctionType {
            /** Paimon default bucket function. */
            DEFAULT,
            /** Paimon mod bucket function. */
            MOD
        }

        private final int bucketNum;
        private final BucketFunctionType bucketFunctionType;
        private final List<ExprId> bucketKeyExprIds;

        public PaimonFixedBucketRouteInfo(int bucketNum, BucketFunctionType bucketFunctionType,
                List<ExprId> bucketKeyExprIds) {
            this.bucketNum = bucketNum;
            this.bucketFunctionType = Objects.requireNonNull(bucketFunctionType, "bucketFunctionType is null");
            this.bucketKeyExprIds = Objects.requireNonNull(bucketKeyExprIds, "bucketKeyExprIds is null");
        }

        public int getBucketNum() {
            return bucketNum;
        }

        public BucketFunctionType getBucketFunctionType() {
            return bucketFunctionType;
        }

        public List<ExprId> getBucketKeyExprIds() {
            return bucketKeyExprIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PaimonFixedBucketRouteInfo)) {
                return false;
            }
            PaimonFixedBucketRouteInfo that = (PaimonFixedBucketRouteInfo) o;
            return bucketNum == that.bucketNum
                    && bucketFunctionType == that.bucketFunctionType
                    && Objects.equals(bucketKeyExprIds, that.bucketKeyExprIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketNum, bucketFunctionType, bucketKeyExprIds);
        }
    }

    private List<ExprId> outputColExprIds;
    private ExternalSinkHashMode externalSinkHashMode = ExternalSinkHashMode.SCALE_WRITER;
    private PaimonFixedBucketRouteInfo paimonFixedBucketRouteInfo;

    public DistributionSpecExternalTableSinkHashPartitioned() {
        super();
    }

    public List<ExprId> getOutputColExprIds() {
        return outputColExprIds;
    }

    public void setOutputColExprIds(List<ExprId> outputColExprIds) {
        this.outputColExprIds = outputColExprIds;
    }

    public ExternalSinkHashMode getExternalSinkHashMode() {
        return externalSinkHashMode;
    }

    public void setExternalSinkHashMode(ExternalSinkHashMode externalSinkHashMode) {
        this.externalSinkHashMode = Objects.requireNonNull(externalSinkHashMode, "externalSinkHashMode is null");
    }

    public PaimonFixedBucketRouteInfo getPaimonFixedBucketRouteInfo() {
        return paimonFixedBucketRouteInfo;
    }

    public void setPaimonFixedBucketRouteInfo(PaimonFixedBucketRouteInfo paimonFixedBucketRouteInfo) {
        this.paimonFixedBucketRouteInfo = paimonFixedBucketRouteInfo;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecExternalTableSinkHashPartitioned;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DistributionSpecExternalTableSinkHashPartitioned)) {
            return false;
        }
        DistributionSpecExternalTableSinkHashPartitioned that =
                (DistributionSpecExternalTableSinkHashPartitioned) o;
        return Objects.equals(outputColExprIds, that.outputColExprIds)
                && externalSinkHashMode == that.externalSinkHashMode
                && Objects.equals(paimonFixedBucketRouteInfo, that.paimonFixedBucketRouteInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputColExprIds, externalSinkHashMode, paimonFixedBucketRouteInfo);
    }
}
