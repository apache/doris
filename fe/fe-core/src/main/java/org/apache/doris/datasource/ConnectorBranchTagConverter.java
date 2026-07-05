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

import org.apache.doris.catalog.info.BranchOptions;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.TagOptions;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.TagChange;

/**
 * Converts the fe-core/nereids branch/tag info types ({@link CreateOrReplaceBranchInfo} etc.) to the neutral
 * connector SPI carriers ({@link BranchChange}/{@link TagChange}/{@link DropRefChange}).
 *
 * <p>Lives in fe-core because it bridges the fe-catalog info types and the SPI DTOs. The mapping is a plain
 * field copy: the {@code Optional<>} options become nullable carrier fields ({@code null} = unset), matching how
 * the connector treats an unset retention knob (left untouched).</p>
 */
public final class ConnectorBranchTagConverter {

    private ConnectorBranchTagConverter() {
    }

    /** Maps {@code BranchOptions.retain/numSnapshots/retention} to the snapshot-management knobs they drive. */
    public static BranchChange toBranchChange(CreateOrReplaceBranchInfo info) {
        BranchOptions options = info.getBranchOptions();
        return new BranchChange(
                info.getBranchName(),
                info.getCreate(),
                info.getReplace(),
                info.getIfNotExists(),
                options.getSnapshotId().orElse(null),
                options.getRetain().orElse(null),
                options.getNumSnapshots().orElse(null),
                options.getRetention().orElse(null));
    }

    public static TagChange toTagChange(CreateOrReplaceTagInfo info) {
        TagOptions options = info.getTagOptions();
        return new TagChange(
                info.getTagName(),
                info.getCreate(),
                info.getReplace(),
                info.getIfNotExists(),
                options.getSnapshotId().orElse(null),
                options.getRetain().orElse(null));
    }

    public static DropRefChange toDropRefChange(DropBranchInfo info) {
        return new DropRefChange(info.getBranchName(), info.getIfExists());
    }

    public static DropRefChange toDropRefChange(DropTagInfo info) {
        return new DropRefChange(info.getTagName(), info.getIfExists());
    }
}
