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

// Adapted from apache/doris-website PR #4043, commit
// 133f948c235995a917b2e1f6d4e9d764b6d62726.

import React, { JSX } from 'react';
import { BaseEdge, StepEdge, type EdgeProps } from '@xyflow/react';
import type { ProfileFlowEdge } from './profileGraphLayout';

export function ProfileGraphEdge(props: EdgeProps<ProfileFlowEdge>): JSX.Element {
    const { data, id, interactionWidth, markerEnd, markerStart, style } = props;

    if (!data?.elkPath) {
        return <StepEdge {...props} />;
    }

    return (
        <BaseEdge
            id={id}
            path={data.elkPath}
            markerStart={markerStart}
            markerEnd={markerEnd}
            interactionWidth={interactionWidth}
            style={style}
        />
    );
}
