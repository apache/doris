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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.spi.Split;

import java.util.List;

/**
 * Consumer-side abstraction for planned split output.
 *
 * <p>A {@link PlanningSplitProducer} writes planned splits into a sink, while the sink decides how
 * to consume them. For example, {@link SplitAssignment} incrementally assigns split batches to
 * backends, and {@link CollectingSplitSink} buffers all splits for synchronous callers.
 */
public interface SplitSink {
    /**
     * Accepts one batch of planned splits.
     */
    void addBatch(List<Split> splits) throws UserException;

    /**
     * Whether the sink still wants more planned splits.
     *
     * <p>Producers should stop early when this returns {@code false}.
     */
    boolean needMore();

    /**
     * Signals that split planning completed successfully and no more batches will be produced.
     */
    void finish();

    /**
     * Signals that split planning failed and no more batches will be produced.
     */
    void fail(UserException e);
}
