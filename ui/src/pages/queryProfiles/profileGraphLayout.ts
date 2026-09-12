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

import { MarkerType, type Edge, type Node } from '@xyflow/react';

import type {
    ProfileDagEdge,
    ProfileDagFragment,
    ProfileDagNode,
    ProfileDagResponse,
} from './profileGraphTypes';

export const OPERATOR_NODE_WIDTH = 224;
export const OPERATOR_NODE_HEIGHT = 104;
export const FRAGMENT_HEADER_HEIGHT = 42;

const DATA_EDGE_COLOR = '#667085';
const DEPENDENCY_EDGE_COLOR = '#d98b00';

export type ProfileFlowNodeData =
    | {
          kind: 'fragment';
          fragmentId: string;
          label: string;
      }
    | {
          kind: 'operator';
          node: ProfileDagNode;
          pipelineLabel: string;
          instanceNum: number | null;
      };

export interface ProfileFlowEdgeData extends Record<string, unknown> {
    kind: ProfileDagEdge['kind'];
    relationId: string | null;
    dependency: boolean;
    crossFragment: boolean;
    elkPath: string | null;
}

export type ProfileFlowNode = Node<ProfileFlowNodeData>;
export type ProfileFlowEdge = Edge<ProfileFlowEdgeData>;

interface ElkNode {
    id: string;
    x?: number;
    y?: number;
    width?: number;
    height?: number;
    children?: ElkNode[];
    layoutOptions?: Record<string, string>;
}

interface ElkEdge {
    id: string;
    sources: string[];
    targets: string[];
    container?: string;
    sections?: ElkEdgeSection[];
}

interface ElkPoint {
    x: number;
    y: number;
}

interface ElkEdgeSection {
    startPoint: ElkPoint;
    bendPoints?: ElkPoint[];
    endPoint: ElkPoint;
}

export interface ElkGraph extends ElkNode {
    children: ElkNode[];
    edges: ElkEdge[];
}

interface ElkLayoutEngine {
    layout(graph: ElkGraph): Promise<ElkGraph>;
}

function isFinitePoint(point: ElkPoint | undefined): point is ElkPoint {
    return point !== undefined && Number.isFinite(point.x) && Number.isFinite(point.y);
}

function simplifyOrthogonalPoints(points: ElkPoint[]): ElkPoint[] {
    const simplified: ElkPoint[] = [];
    for (const point of points) {
        const previous = simplified[simplified.length - 1];
        if (previous && previous.x === point.x && previous.y === point.y) continue;

        const beforePrevious = simplified[simplified.length - 2];
        if (
            beforePrevious &&
            previous &&
            ((beforePrevious.x === previous.x && previous.x === point.x) ||
                (beforePrevious.y === previous.y && previous.y === point.y))
        ) {
            simplified[simplified.length - 1] = point;
        } else {
            simplified.push(point);
        }
    }
    return simplified;
}

function elkEdgePath(edge: ElkEdge | undefined, containerOffset: ElkPoint | undefined): string | null {
    if (!edge?.sections?.length || !containerOffset) return null;
    const subpaths: string[] = [];
    for (const section of edge.sections) {
        const points = [section.startPoint, ...(section.bendPoints ?? []), section.endPoint];
        if (!points.every(isFinitePoint)) return null;
        const absolutePoints = simplifyOrthogonalPoints(
            points.map(point => ({
                x: point.x + containerOffset.x,
                y: point.y + containerOffset.y,
            })),
        );
        if (absolutePoints.length < 2) return null;
        subpaths.push(
            absolutePoints
                .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`)
                .join(' '),
        );
    }
    return subpaths.join(' ');
}

export function isDependencyEdge(kind: ProfileDagEdge['kind']): boolean {
    return kind === 'BUILD_DEPENDENCY' || kind === 'BLOCKING_DEPENDENCY';
}

export function formatDurationNs(value: number | null | undefined): string {
    if (value == null) return 'Unknown';
    if (value < 1_000) return `${value} ns`;
    if (value < 1_000_000) return `${formatDecimal(value / 1_000)} µs`;
    if (value < 1_000_000_000) return `${formatDecimal(value / 1_000_000)} ms`;
    return `${formatDecimal(value / 1_000_000_000)} s`;
}

export function formatCount(value: number | null | undefined): string {
    if (value == null) return 'Unknown';
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
}

export function formatBytes(value: number | null | undefined): string {
    if (value == null) return 'Unknown';
    if (value < 1024) return `${value} B`;
    const units = ['KiB', 'MiB', 'GiB', 'TiB'];
    let amount = value / 1024;
    let unitIndex = 0;
    while (amount >= 1024 && unitIndex < units.length - 1) {
        amount /= 1024;
        unitIndex += 1;
    }
    return `${formatDecimal(amount)} ${units[unitIndex]}`;
}

function formatDecimal(value: number): string {
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
}

function fragmentNumber(fragment: ProfileDagFragment): number {
    return fragment.number;
}

function pipelineNumber(pipelineId: string): string {
    const match = /\/pipeline:(\d+)$/.exec(pipelineId);
    return match?.[1] ?? pipelineId;
}

export function fragmentLabel(fragmentId: string): string {
    const match = /^fragment:(\d+)$/.exec(fragmentId);
    return match ? `Fragment ${match[1]}` : fragmentId;
}

export const DEFAULT_HOTSPOT_LIMIT = 5;

export interface ProfileHotspot {
    /** Graph node id, used to focus the operator on the canvas. */
    id: string;
    label: string;
    location: string;
    planNodeId: number | null;
    execMaxNs: number;
}

/**
 * Ranks operators by maximum execution time, the same metric that drives the node
 * heat colors and the summary bottleneck, so the list and the graph always agree.
 */
export function selectSlowestOperators(
    dag: ProfileDagResponse,
    limit: number = DEFAULT_HOTSPOT_LIMIT,
): ProfileHotspot[] {
    const ranked: ProfileHotspot[] = [];
    for (const node of dag.graph.nodes) {
        const execMaxNs = node.timing?.execTime?.maxNs;
        if (typeof execMaxNs !== 'number' || !Number.isFinite(execMaxNs) || execMaxNs <= 0) continue;
        ranked.push({
            id: node.id,
            label: node.label,
            location: `${fragmentLabel(node.fragmentId)} · Pipeline ${pipelineNumber(node.pipelineId)}`,
            planNodeId: node.planNodeId ?? null,
            execMaxNs,
        });
    }
    // Slowest first; the node id keeps ties in a stable order across renders.
    ranked.sort((left, right) => right.execMaxNs - left.execMaxNs || left.id.localeCompare(right.id));
    return ranked.slice(0, Math.max(0, limit));
}

export function buildElkGraph(dag: ProfileDagResponse): ElkGraph {
    const nodesByFragment = new Map<string, ProfileDagNode[]>();
    for (const node of dag.graph.nodes) {
        const current = nodesByFragment.get(node.fragmentId) ?? [];
        current.push(node);
        nodesByFragment.set(node.fragmentId, current);
    }

    const fragments = [...dag.fragments].sort((left, right) => fragmentNumber(left) - fragmentNumber(right));
    const knownFragmentIds = new Set(fragments.map(fragment => fragment.id));
    for (const fragmentId of nodesByFragment.keys()) {
        if (!knownFragmentIds.has(fragmentId)) {
            fragments.push({ id: fragmentId, number: Number.MAX_SAFE_INTEGER, pipelineIds: [], nodeIds: [] });
        }
    }

    return {
        id: 'profile-dag',
        layoutOptions: {
            'elk.algorithm': 'layered',
            'elk.direction': 'UP',
            'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
            'elk.edgeRouting': 'ORTHOGONAL',
            'elk.layered.spacing.nodeNodeBetweenLayers': '90',
            'elk.spacing.nodeNode': '48',
            'elk.spacing.componentComponent': '64',
            'elk.padding': '[top=54,left=28,bottom=28,right=28]',
        },
        children: fragments.map(fragment => ({
            id: fragment.id,
            layoutOptions: {
                'elk.padding': `[top=${FRAGMENT_HEADER_HEIGHT + 16},left=20,bottom=20,right=20]`,
            },
            children: (nodesByFragment.get(fragment.id) ?? []).map(node => ({
                id: node.id,
                width: OPERATOR_NODE_WIDTH,
                height: OPERATOR_NODE_HEIGHT,
            })),
        })),
        edges: dag.graph.edges.map(edge => ({
            id: edge.id,
            sources: [edge.source],
            targets: [edge.target],
        })),
    };
}

export async function layoutProfileDag(
    dag: ProfileDagResponse,
    engine?: ElkLayoutEngine,
): Promise<{ nodes: ProfileFlowNode[]; edges: ProfileFlowEdge[] }> {
    const elk = engine ?? (await createElkEngine());
    const laidOut = await elk.layout(buildElkGraph(dag));
    const fragmentById = new Map(dag.fragments.map(fragment => [fragment.id, fragment]));
    const pipelineById = new Map(dag.pipelines.map(pipeline => [pipeline.id, pipeline]));
    const sourceNodeById = new Map(dag.graph.nodes.map(node => [node.id, node]));
    const laidOutEdgeById = new Map((laidOut.edges ?? []).map(edge => [edge.id, edge]));
    const containerOffsetById = new Map<string, ElkPoint>([
        [laidOut.id, { x: 0, y: 0 }],
        ...(laidOut.children ?? []).map(
            fragment => [fragment.id, { x: fragment.x ?? 0, y: fragment.y ?? 0 }] as const,
        ),
    ]);
    const flowNodes: ProfileFlowNode[] = [];

    for (const fragmentLayout of laidOut.children ?? []) {
        const fragment = fragmentById.get(fragmentLayout.id);
        flowNodes.push({
            id: fragmentLayout.id,
            type: 'profileFragment',
            position: { x: fragmentLayout.x ?? 0, y: fragmentLayout.y ?? 0 },
            style: { width: fragmentLayout.width ?? OPERATOR_NODE_WIDTH + 40, height: fragmentLayout.height ?? 180 },
            data: {
                kind: 'fragment',
                fragmentId: fragmentLayout.id,
                label: fragment ? `Fragment ${fragment.number}` : fragmentLayout.id,
            },
            draggable: false,
            selectable: false,
            connectable: false,
        });

        for (const operatorLayout of fragmentLayout.children ?? []) {
            const node = sourceNodeById.get(operatorLayout.id);
            if (!node) continue;
            const pipeline = pipelineById.get(node.pipelineId);
            flowNodes.push({
                id: node.id,
                type: 'profileOperator',
                parentId: fragmentLayout.id,
                extent: 'parent',
                position: { x: operatorLayout.x ?? 0, y: operatorLayout.y ?? FRAGMENT_HEADER_HEIGHT },
                width: OPERATOR_NODE_WIDTH,
                height: OPERATOR_NODE_HEIGHT,
                data: {
                    kind: 'operator',
                    node,
                    pipelineLabel: `Pipeline ${pipeline?.number ?? pipelineNumber(node.pipelineId)}`,
                    instanceNum: pipeline?.instanceNum ?? null,
                },
                draggable: false,
                selectable: true,
                connectable: false,
            });
        }
    }

    return {
        nodes: flowNodes,
        edges: dag.graph.edges.map(edge => {
            const dependency = isDependencyEdge(edge.kind);
            const laidOutEdge = laidOutEdgeById.get(edge.id);
            const elkPath = elkEdgePath(
                laidOutEdge,
                containerOffsetById.get(laidOutEdge?.container ?? laidOut.id),
            );
            return {
                id: edge.id,
                source: edge.source,
                target: edge.target,
                type: 'profileElk',
                animated: false,
                selectable: false,
                reconnectable: false,
                style: {
                    stroke: dependency ? DEPENDENCY_EDGE_COLOR : DATA_EDGE_COLOR,
                    strokeWidth: dependency ? 1.5 : 2,
                    strokeDasharray: dependency ? '7 5' : undefined,
                },
                markerEnd: {
                    type: dependency ? MarkerType.Arrow : MarkerType.ArrowClosed,
                    color: dependency ? DEPENDENCY_EDGE_COLOR : DATA_EDGE_COLOR,
                    width: 10,
                    height: 10,
                    strokeWidth: dependency ? 1.5 : 1,
                },
                data: {
                    kind: edge.kind,
                    relationId: edge.relationId ?? null,
                    dependency,
                    crossFragment: edge.metadata?.crossFragment === true,
                    elkPath,
                },
            };
        }),
    };
}

async function createElkEngine(): Promise<ElkLayoutEngine> {
    const module = await import('elkjs/lib/elk.bundled.js');
    const Elk = module.default;
    return new Elk() as unknown as ElkLayoutEngine;
}
