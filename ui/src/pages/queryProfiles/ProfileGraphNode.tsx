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

import React, { CSSProperties, JSX } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ProfileFlowNode } from './profileGraphLayout';
import { formatDurationNs } from './profileGraphLayout';

type HeatStyle = CSSProperties & {
    '--profile-dag-heat-border'?: string;
    '--profile-dag-heat-background'?: string;
    '--profile-dag-wait-width'?: string;
};

export function ProfileGraphNode({ data }: NodeProps<ProfileFlowNode>): JSX.Element {
    if (data.kind !== 'operator') {
        return <div />;
    }

    const { node, pipelineLabel, instanceNum } = data;
    const execTime = node.timing?.execTime;
    const waitTime = node.timing?.waitTime;
    const heat = Math.pow(node.analysis?.heat ?? 0, 0.6);
    const waitHeat = node.analysis?.waitHeat ?? 0;
    const style: HeatStyle = {
        '--profile-dag-heat-border': `${Math.round(heat * 70)}%`,
        '--profile-dag-heat-background': `${Math.round(heat * 22)}%`,
        '--profile-dag-wait-width': `${Math.round(waitHeat * 100)}%`,
    };

    return (
        <article
            className={`profile-dag-node${node.analysis?.isBottleneck ? ' profile-dag-node--bottleneck' : ''}`}
            style={style}
            aria-label={`${node.label} operator`}
        >
            <Handle className="profile-dag-node__handle" type="source" position={Position.Top} isConnectable={false} />
            <Handle className="profile-dag-node__handle" type="target" position={Position.Bottom} isConnectable={false} />
            <div className="profile-dag-node__heading">
                <strong title={node.operatorType}>{node.label}</strong>
                {node.analysis?.isBottleneck && <span className="profile-dag-node__bottleneck">Bottleneck</span>}
            </div>
            <div className="profile-dag-node__location">
                <span>{node.fragmentId.replace('fragment:', 'Fragment ')}</span>
                <span>{pipelineLabel}</span>
                {instanceNum !== null && <span>{instanceNum} instances</span>}
            </div>
            <dl className="profile-dag-node__timing">
                <div>
                    <dt>Exec max</dt>
                    <dd>{formatDurationNs(execTime?.maxNs ?? null)}</dd>
                </div>
                <div>
                    <dt>Exec avg</dt>
                    <dd>{formatDurationNs(execTime?.avgNs ?? null)}</dd>
                </div>
                <div>
                    <dt>Wait max</dt>
                    <dd>{formatDurationNs(waitTime?.maxNs ?? null)}</dd>
                </div>
            </dl>
            <div className="profile-dag-node__wait" aria-hidden="true" />
        </article>
    );
}
export function ProfileGraphFragmentNode({ data }: NodeProps<ProfileFlowNode>): JSX.Element {
    if (data.kind !== 'fragment') {
        return <div />;
    }

    return (
        <section className="profile-dag-fragment" aria-label={data.label}>
            <span>{data.label}</span>
        </section>
    );
}
