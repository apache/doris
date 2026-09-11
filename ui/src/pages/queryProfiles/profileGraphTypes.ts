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

export type DagUiState = 'idle' | 'parsing' | 'ready' | 'unavailable' | 'failed';

export type DagOperatorRole =
    | 'SOURCE'
    | 'SINK'
    | 'PROBE'
    | 'BUILD'
    | 'PRODUCER'
    | 'CONSUMER'
    | 'UNKNOWN';

export interface DagAggregateMetric {
    sum?: number | null;
    avg?: number | null;
    max?: number | null;
    min?: number | null;
}

export interface DagExecTime {
    sumNs?: number | null;
    avgNs?: number | null;
    maxNs?: number | null;
    minNs?: number | null;
    display?: string;
}

export interface DagWaitTime {
    totalNs?: number | null;
    maxNs?: number | null;
    avgNs?: number | null;
    display?: string;
    breakdown?: Record<string, number | null>;
}

export interface ProfileDagNode {
    id: string;
    fragmentId: string;
    pipelineId: string;
    ordinal: number;
    operatorType: string;
    operatorFamily: string;
    role: DagOperatorRole;
    label: string;
    planNodeId?: number | null;
    nereidsId?: number | null;
    destId?: number | null;
    destIds: number[];
    known: boolean;
    lineNumber: number;
    headerAttributes?: Record<string, string | number | boolean | Array<string | number | boolean>>;
    planInfo: Record<string, string | number | boolean | Array<string | number | boolean>>;
    timing?: {
        execTime?: DagExecTime;
        waitTime?: DagWaitTime;
    };
    metrics?: Record<string, DagAggregateMetric | null>;
    analysis?: {
        heat?: number | null;
        waitHeat?: number | null;
        isBottleneck?: boolean;
    };
}

export type DagEdgeKind =
    | 'PIPELINE_DATA'
    | 'EXCHANGE'
    | 'LOCAL_EXCHANGE'
    | 'MULTICAST'
    | 'BUILD_DEPENDENCY'
    | 'BLOCKING_DEPENDENCY';

export interface ProfileDagEdge {
    id: string;
    kind: DagEdgeKind;
    source: string;
    target: string;
    relationId?: string | null;
    resolved: true;
    metadata?: Record<string, string | number | boolean | null>;
}

export interface ProfileDagFragment {
    id: string;
    number: number;
    pipelineIds: string[];
    nodeIds: string[];
}

export interface ProfileDagPipeline {
    id: string;
    fragmentId: string;
    number: number;
    instanceNum: number;
    nodeIds: string[];
    waitWorkerTime?: Record<string, number | null>;
}

export interface ProfileDagUnresolvedReference {
    kind: string;
    relationId?: string | null;
    sourceNodeId: string;
    reason: string;
}

export interface ProfileDagWarning {
    kind?: string;
    code?: string;
    nodeId?: string;
    operatorType?: string;
    message?: string;
    lineNumber?: number;
}

export interface ProfileDagSummary {
    fragmentCount: number;
    pipelineCount: number;
    nodeCount: number;
    edgeCount: number;
    unresolvedEdgeCount: number;
    criticalNodeId?: string | null;
    maxExecTimeNs?: number | null;
    maxWaitTimeNs?: number | null;
}

export interface ProfileGraphIR {
    schemaVersion: '1.0';
    parserVersion?: string;
    jobId?: string;
    profile: Record<string, unknown>;
    graph: {
        direction: 'BOTTOM_TO_TOP';
        nodes: ProfileDagNode[];
        edges: ProfileDagEdge[];
    };
    fragments: ProfileDagFragment[];
    pipelines: ProfileDagPipeline[];
    unresolvedReferences: ProfileDagUnresolvedReference[];
    warnings: ProfileDagWarning[];
    summary: ProfileDagSummary;
}

export type ProfileDag = ProfileGraphIR;
export type ProfileDagResponse = ProfileGraphIR;
