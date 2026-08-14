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

import type {
    DagAggregateMetric,
    DagEdgeKind,
    DagOperatorRole,
    ProfileDagEdge,
    ProfileDagFragment,
    ProfileDagNode,
    ProfileDagPipeline,
    ProfileDagUnresolvedReference,
    ProfileDagWarning,
    ProfileGraphIR,
} from './profileGraphTypes';

export const PROFILE_PARSER_VERSION = '0.2.0-ts.1';
export const MAX_PARSER_BYTES = 10 * 1024 * 1024;
export const MAX_PARSER_LINES = 200_000;
export const MAX_PARSER_LINE_BYTES = 64 * 1024;
export const MAX_DAG_NODES = 500;
export const MAX_DAG_EDGES = 1_000;
const MAX_FRAGMENTS = 512;
const MAX_PIPELINES = 4_096;
const MAX_PLAN_INFO_VALUE_LENGTH = 300;

export type ProfileParserErrorCode =
    | 'DAG_UNAVAILABLE'
    | 'DAG_TOO_LARGE'
    | 'DAG_PARSE_FAILED';

export class ProfileParserError extends Error {
    constructor(public readonly code: ProfileParserErrorCode, message: string) {
        super(message);
        this.name = 'ProfileParserError';
    }
}

const MERGED_RE = /^\s*MergedProfile:\s*$/;
const DETAIL_RE = /^\s*DetailProfile(?:\([^)]*\))?:\s*$/;
const EXEC_PROFILE_RE = /^\s*Execution\s+Profile/;
const FRAGMENT_RE = /^\s*Fragment\s+(\d+):\s*$/;
const PIPELINE_RE = /^\s*Pipeline\s+(\d+)\s*\(\s*instance_num\s*=\s*(\d+)\s*\):\s*$/;
const OPERATOR_RE = /^\s+([A-Z][A-Z0-9_]*_OPERATOR)(.*):\s*$/;
const ATTRIBUTE_RE = /([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(-?\d+)/g;
const TABLE_NAME_RE = /table_name=([^,)]+(?:\([^)]*\))?)/;
const COUNTERS_RE = /^\s*(?:Common|Custom)Counters:\s*$/;
const PLANINFO_RE = /^\s*-\s*PlanInfo\s*$/;
const COUNTER_RE = /^\s*-\s*([A-Za-z0-9_[\]]+?)\s*:\s*(.+?)\s*$/;
const PLAN_KV_RE = /^\s*-\s*([^:=]+?)\s*[:=]\s*(.+?)\s*$/;

type OperatorSpec = readonly [family: string, role: DagOperatorRole, label: string];

export const OPERATOR_SPECS: Readonly<Record<string, OperatorSpec>> = {
    RESULT_SINK_OPERATOR: ['RESULT', 'SINK', 'RESULT'],
    DATA_STREAM_SINK_OPERATOR: ['REMOTE_EXCHANGE', 'SINK', 'DATA STREAM'],
    EXCHANGE_OPERATOR: ['REMOTE_EXCHANGE', 'SOURCE', 'EXCHANGE'],
    LOCAL_EXCHANGE_OPERATOR: ['LOCAL_EXCHANGE', 'SOURCE', 'LOCAL EXCHANGE'],
    LOCAL_EXCHANGE_SINK_OPERATOR: ['LOCAL_EXCHANGE', 'SINK', 'LOCAL EXCHANGE SINK'],
    HASH_JOIN_OPERATOR: ['HASH_JOIN', 'PROBE', 'HASH JOIN'],
    HASH_JOIN_SINK_OPERATOR: ['HASH_JOIN', 'BUILD', 'HASH JOIN BUILD'],
    PARTITIONED_HASH_JOIN_OPERATOR: ['PARTITIONED_HASH_JOIN', 'PROBE', 'PARTITIONED HASH JOIN'],
    PARTITIONED_HASH_JOIN_SINK_OPERATOR: ['PARTITIONED_HASH_JOIN', 'BUILD', 'PARTITIONED HASH JOIN BUILD'],
    NESTED_LOOP_JOIN_OPERATOR: ['CROSS_JOIN', 'PROBE', 'NESTED LOOP JOIN'],
    NESTED_LOOP_JOIN_SINK_OPERATOR: ['CROSS_JOIN', 'BUILD', 'NESTED LOOP JOIN BUILD'],
    CROSS_JOIN_OPERATOR: ['CROSS_JOIN', 'PROBE', 'CROSS JOIN'],
    CROSS_JOIN_SINK_OPERATOR: ['CROSS_JOIN', 'BUILD', 'CROSS JOIN BUILD'],
    AGGREGATION_OPERATOR: ['AGGREGATION', 'SOURCE', 'AGGREGATION'],
    AGGREGATION_SINK_OPERATOR: ['AGGREGATION', 'SINK', 'AGGREGATION SINK'],
    STREAMING_AGGREGATION_OPERATOR: ['STREAMING_AGGREGATION', 'SOURCE', 'STREAMING AGGREGATION'],
    DISTINCT_STREAMING_AGGREGATION_OPERATOR: ['STREAMING_AGGREGATION', 'SOURCE', 'DISTINCT STREAMING AGGREGATION'],
    SORT_OPERATOR: ['SORT', 'SOURCE', 'SORT'],
    SORT_SINK_OPERATOR: ['SORT', 'SINK', 'SORT SINK'],
    LOCAL_MERGE_SORT_SOURCE_OPERATOR: ['SORT', 'SOURCE', 'LOCAL MERGE SORT'],
    PARTITION_SORT_OPERATOR: ['PARTITION_SORT', 'SOURCE', 'PARTITION SORT'],
    PARTITION_SORT_SINK_OPERATOR: ['PARTITION_SORT', 'SINK', 'PARTITION SORT SINK'],
    ANALYTIC_EVAL_OPERATOR: ['ANALYTIC_EVAL', 'SOURCE', 'ANALYTIC EVAL'],
    ANALYTIC_EVAL_SINK_OPERATOR: ['ANALYTIC_EVAL', 'SINK', 'ANALYTIC EVAL SINK'],
    MULTI_CAST_DATA_STREAM_SINK_OPERATOR: ['MULTICAST', 'PRODUCER', 'MULTI CAST PRODUCER'],
    MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR: ['MULTICAST', 'CONSUMER', 'MULTI CAST CONSUMER'],
    OLAP_SCAN_OPERATOR: ['SCAN', 'SOURCE', 'OLAP SCAN'],
    FILE_SCAN_OPERATOR: ['SCAN', 'SOURCE', 'FILE SCAN'],
    GROUP_COMMIT_SCAN_OPERATOR: ['SCAN', 'SOURCE', 'GROUP COMMIT SCAN'],
    ES_SCAN_OPERATOR: ['SCAN', 'SOURCE', 'ES SCAN'],
    JDBC_SCAN_OPERATOR: ['SCAN', 'SOURCE', 'JDBC SCAN'],
    SELECT_OPERATOR: ['SELECT', 'SOURCE', 'SELECT'],
    UNION_OPERATOR: ['UNION', 'SOURCE', 'UNION'],
    UNION_SINK_OPERATOR: ['UNION', 'SINK', 'UNION SINK'],
    REPEAT_OPERATOR: ['REPEAT', 'SOURCE', 'REPEAT'],
    TABLE_FUNCTION_OPERATOR: ['TABLE_FUNCTION', 'SOURCE', 'TABLE FUNCTION'],
    ASSERT_NUM_ROWS_OPERATOR: ['ASSERT_NUM_ROWS', 'SOURCE', 'ASSERT NUM ROWS'],
};

const PLAN_INFO_WHITELIST: Readonly<Record<string, string>> = {
    table: 'table',
    table_name: 'table',
    'join op': 'joinOp',
    'equal join conjunct': 'joinConjunct',
    'other join predicates': 'joinOtherPredicates',
    cardinality: 'cardinality',
    'group by': 'groupBy',
    'order by': 'orderBy',
    limit: 'limit',
    offset: 'offset',
    algorithm: 'algorithm',
    'runtime filters': 'runtimeFilters',
    partitions: 'partitions',
    tablet: 'tablets',
};

type CounterValues = Record<'sum' | 'avg' | 'max' | 'min', number | undefined>;

interface ParsedNode extends ProfileDagNode {
    counters: Record<string, CounterValues>;
}

type PairingRule = readonly [
    family: string,
    sourceRoles: readonly DagOperatorRole[],
    targetRoles: readonly DagOperatorRole[],
    kind: DagEdgeKind,
];

const PAIRING_RULES: readonly PairingRule[] = [
    ['LOCAL_EXCHANGE', ['SINK'], ['SOURCE'], 'LOCAL_EXCHANGE'],
    ['HASH_JOIN', ['BUILD'], ['PROBE'], 'BUILD_DEPENDENCY'],
    ['PARTITIONED_HASH_JOIN', ['BUILD'], ['PROBE'], 'BUILD_DEPENDENCY'],
    ['CROSS_JOIN', ['BUILD'], ['PROBE'], 'BUILD_DEPENDENCY'],
    ['AGGREGATION', ['SINK'], ['SOURCE'], 'BLOCKING_DEPENDENCY'],
    ['SORT', ['SINK'], ['SOURCE'], 'BLOCKING_DEPENDENCY'],
    ['PARTITION_SORT', ['SINK'], ['SOURCE'], 'BLOCKING_DEPENDENCY'],
    ['ANALYTIC_EVAL', ['SINK'], ['SOURCE'], 'BLOCKING_DEPENDENCY'],
    ['UNION', ['SINK'], ['SOURCE'], 'BLOCKING_DEPENDENCY'],
];

const utf8Encoder = new TextEncoder();

function fail(code: ProfileParserErrorCode, message: string): never {
    throw new ProfileParserError(code, message);
}

function utf8Length(value: string): number {
    return utf8Encoder.encode(value).byteLength;
}

function safeInteger(value: number, code: ProfileParserErrorCode = 'DAG_PARSE_FAILED'): number {
    if (!Number.isSafeInteger(value) || value < 0) {
        fail(code, 'The Profile contains a metric outside the supported numeric range.');
    }
    return value;
}

function parseAttributes(value: string): Record<string, number[]> {
    const attributes: Record<string, number[]> = {};
    for (const match of value.matchAll(ATTRIBUTE_RE)) {
        const parsed = Number(match[2]);
        if (!Number.isSafeInteger(parsed)) continue;
        (attributes[match[1]] ??= []).push(parsed);
    }
    return attributes;
}

function counterKind(name: string): 'time' | 'bytes' | 'count' {
    if (name === 'ExecTime' || name.includes('Time')) return 'time';
    if (name.includes('Memory') || name.includes('Bytes')) return 'bytes';
    return 'count';
}

function parseTimeToken(token: string): number | null {
    const compact = token.replace(/\s+/g, '');
    const part = /([0-9]+(?:\.[0-9]+)?)(ns|us|µs|ms|sec|s|min|h)/g;
    const factors: Record<string, number> = {
        ns: 1,
        us: 1e3,
        'µs': 1e3,
        ms: 1e6,
        sec: 1e9,
        s: 1e9,
        min: 60e9,
        h: 3_600e9,
    };
    let total = 0;
    let consumed = '';
    for (const match of compact.matchAll(part)) {
        consumed += match[0];
        total += Number(match[1]) * factors[match[2]];
    }
    if (!consumed || consumed !== compact || !Number.isFinite(total)) return null;
    return safeInteger(Math.round(total));
}

function parseToken(token: string, kind: ReturnType<typeof counterKind>): number | null {
    if (kind === 'time') return parseTimeToken(token);
    if (kind === 'bytes') {
        const match = token.match(/^([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB|TB)?\s*$/);
        if (!match) return null;
        const factor = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 }[
            match[2] as 'B' | 'KB' | 'MB' | 'GB' | 'TB'
        ] ?? 1;
        return safeInteger(Math.round(Number(match[1]) * factor));
    }
    const match = token.match(/^([0-9]+(?:\.[0-9]+)?)\s*([KMGB]?)(?:\s*\(([0-9]+)\))?\s*$/);
    if (!match) return null;
    if (match[3]) return safeInteger(Number(match[3]));
    const factor = { '': 1, K: 1e3, M: 1e6, G: 1e9, B: 1e9 }[match[2]] ?? 1;
    return safeInteger(Math.round(Number(match[1]) * factor));
}

export function parseCounter(name: string, value: string): CounterValues {
    const values: CounterValues = { sum: undefined, avg: undefined, max: undefined, min: undefined };
    const kind = counterKind(name);
    for (const part of value.split(',')) {
        const match = part.trim().match(/^(sum|avg|max|min)\s+(.+)$/);
        if (!match) continue;
        const parsed = parseToken(match[2].trim(), kind);
        if (parsed !== null) values[match[1] as keyof CounterValues] = parsed;
    }
    return values;
}

function humanizeNanoseconds(value: number): string {
    if (value >= 1e9) return `${(value / 1e9).toFixed(2)}s`;
    if (value >= 1e6) return `${(value / 1e6).toFixed(2)}ms`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(2)}us`;
    return `${value}ns`;
}

function aggregateMetric(counter: CounterValues | undefined): DagAggregateMetric | null {
    if (!counter || Object.values(counter).every(value => value === undefined)) return null;
    return {
        sum: counter.sum ?? null,
        avg: counter.avg ?? null,
        max: counter.max ?? null,
        min: counter.min ?? null,
    };
}

function finalizeNode(node: ParsedNode): ProfileDagNode {
    const exec = node.counters.ExecTime;
    const waitCounters = Object.entries(node.counters).filter(([name]) => name.startsWith('WaitFor') && name.endsWith('Time'));
    const waitMaxValues = waitCounters.map(([, counter]) => counter.max).filter((value): value is number => value !== undefined);
    const waitAverageValues = waitCounters.map(([, counter]) => counter.avg).filter((value): value is number => value !== undefined);
    const breakdown = {
        waitForDependencyNs: 0,
        waitForDataNs: 0,
        waitForRpcBufferQueueNs: 0,
    };
    for (const [name, counter] of waitCounters) {
        const value = counter.max ?? 0;
        if (name.startsWith('WaitForRpcBufferQueue')) breakdown.waitForRpcBufferQueueNs += value;
        else if (name.startsWith('WaitForData')) breakdown.waitForDataNs += value;
        else breakdown.waitForDependencyNs += value;
    }

    const timing: ProfileDagNode['timing'] = {};
    if (exec && Object.values(exec).some(value => value !== undefined)) {
        timing.execTime = {
            sumNs: exec.sum ?? null,
            avgNs: exec.avg ?? null,
            maxNs: exec.max ?? null,
            minNs: exec.min ?? null,
            display:
                exec.max !== undefined && exec.avg !== undefined
                    ? `max ${humanizeNanoseconds(exec.max)} / avg ${humanizeNanoseconds(exec.avg)}`
                    : undefined,
        };
    }
    if (waitCounters.length > 0) {
        const maxNs = waitMaxValues.length > 0 ? Math.max(...waitMaxValues) : 0;
        const totalNs = waitMaxValues.reduce((total, value) => total + value, 0);
        const avgNs =
            waitAverageValues.length > 0
                ? Math.round(waitAverageValues.reduce((total, value) => total + value, 0) / waitAverageValues.length)
                : 0;
        timing.waitTime = {
            totalNs: safeInteger(totalNs),
            maxNs,
            avgNs: safeInteger(avgNs),
            display: `max ${humanizeNanoseconds(maxNs)}`,
            breakdown,
        };
    }

    const { counters: _counters, ...output } = node;
    void _counters;
    return {
        ...output,
        timing,
        metrics: {
            inputRows: aggregateMetric(node.counters.RowsProduced),
            outputRows: aggregateMetric(node.counters.RowsReturned),
            memoryUsageBytes: aggregateMetric(node.counters.MemoryUsage),
            memoryPeakBytes: aggregateMetric(node.counters.MemoryUsagePeak),
        },
        analysis: { heat: null, waitHeat: null, isBottleneck: false },
    };
}

function relationKey(fragmentId: string, family: string, idKind: 'nereids' | 'plan', id: number): string {
    return `${fragmentId}\0${family}\0${idKind}\0${id}`;
}

function validateGraph(nodes: ProfileDagNode[], edges: ProfileDagEdge[]): void {
    const nodeIds = new Set(nodes.map(node => node.id));
    if (nodeIds.size !== nodes.length) fail('DAG_PARSE_FAILED', 'The execution graph contains duplicate node IDs.');

    const outgoing = new Map<string, string[]>();
    const indegree = new Map(nodes.map(node => [node.id, 0]));
    const edgeKeys = new Set<string>();
    for (const edge of edges) {
        if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target) || edge.source === edge.target) {
            fail('DAG_PARSE_FAILED', 'The execution graph contains an invalid edge.');
        }
        const key = `${edge.source}\0${edge.target}\0${edge.kind}`;
        if (edgeKeys.has(key)) fail('DAG_PARSE_FAILED', 'The execution graph contains a duplicate edge.');
        edgeKeys.add(key);
        const targets = outgoing.get(edge.source);
        if (targets) targets.push(edge.target);
        else outgoing.set(edge.source, [edge.target]);
        indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
    }

    const queue = [...indegree.entries()].filter(([, degree]) => degree === 0).map(([id]) => id);
    let visited = 0;
    for (let index = 0; index < queue.length; index += 1) {
        const id = queue[index];
        visited += 1;
        for (const target of outgoing.get(id) ?? []) {
            const next = (indegree.get(target) ?? 0) - 1;
            indegree.set(target, next);
            if (next === 0) queue.push(target);
        }
    }
    if (visited !== nodes.length) fail('DAG_PARSE_FAILED', 'The execution graph contains a cycle.');
}

function addPerformanceAnalysis(nodes: ProfileDagNode[]): {
    criticalNodeId: string | null;
    maxExecTimeNs: number | null;
    maxWaitTimeNs: number | null;
} {
    const execNodes = nodes.filter(node => node.timing?.execTime?.maxNs !== null && node.timing?.execTime?.maxNs !== undefined);
    const waitNodes = nodes.filter(node => node.timing?.waitTime?.maxNs !== null && node.timing?.waitTime?.maxNs !== undefined);
    const maxExecTimeNs = execNodes.length > 0 ? Math.max(...execNodes.map(node => node.timing?.execTime?.maxNs as number)) : null;
    const maxWaitTimeNs = waitNodes.length > 0 ? Math.max(...waitNodes.map(node => node.timing?.waitTime?.maxNs as number)) : null;
    const criticalNodeId =
        maxExecTimeNs !== null ? execNodes.find(node => node.timing?.execTime?.maxNs === maxExecTimeNs)?.id ?? null : null;

    for (const node of nodes) {
        const execMax = node.timing?.execTime?.maxNs;
        const waitMax = node.timing?.waitTime?.maxNs;
        node.analysis = {
            heat:
                execMax === null || execMax === undefined || maxExecTimeNs === null || maxExecTimeNs === 0
                    ? null
                    : Math.round((execMax / maxExecTimeNs) * 10_000) / 10_000,
            waitHeat:
                waitMax === null || waitMax === undefined || maxWaitTimeNs === null || maxWaitTimeNs === 0
                    ? null
                    : Math.round((waitMax / maxWaitTimeNs) * 10_000) / 10_000,
            isBottleneck: maxExecTimeNs !== null && maxExecTimeNs > 0 && node.id === criticalNodeId,
        };
    }
    return { criticalNodeId, maxExecTimeNs, maxWaitTimeNs };
}

export function parseProfileText(text: string): ProfileGraphIR {
    if (utf8Length(text) > MAX_PARSER_BYTES) fail('DAG_TOO_LARGE', 'The prepared Profile is larger than 10 MiB.');
    const lines = text.split(/\r?\n/);
    if (lines.length > MAX_PARSER_LINES) fail('DAG_TOO_LARGE', 'The Profile contains too many lines.');

    const fragments: ProfileDagFragment[] = [];
    const pipelines: ProfileDagPipeline[] = [];
    const parsedNodes: ParsedNode[] = [];
    const warnings: ProfileDagWarning[] = [];
    let inMerged = false;
    let currentFragment: ProfileDagFragment | null = null;
    let currentPipeline: ProfileDagPipeline | null = null;
    let currentNode: ParsedNode | null = null;
    let section: 'plan' | 'counters' | null = null;

    for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index];
        if (line.length > MAX_PARSER_LINE_BYTES || (line.length > MAX_PARSER_LINE_BYTES / 4 && utf8Length(line) > MAX_PARSER_LINE_BYTES)) {
            fail('DAG_TOO_LARGE', 'The Profile contains a line larger than 64 KiB.');
        }
        if (!inMerged) {
            if (MERGED_RE.test(line)) inMerged = true;
            continue;
        }
        if (DETAIL_RE.test(line) || EXEC_PROFILE_RE.test(line)) break;

        const fragmentMatch = line.match(FRAGMENT_RE);
        if (fragmentMatch) {
            const number = Number(fragmentMatch[1]);
            currentFragment = {
                id: `fragment:${number}`,
                number,
                pipelineIds: [],
                nodeIds: [],
            };
            fragments.push(currentFragment);
            if (fragments.length > MAX_FRAGMENTS) fail('DAG_TOO_LARGE', 'The Profile contains too many Fragments.');
            currentPipeline = null;
            currentNode = null;
            section = null;
            continue;
        }

        const pipelineMatch = line.match(PIPELINE_RE);
        if (pipelineMatch && currentFragment) {
            const number = Number(pipelineMatch[1]);
            currentPipeline = {
                id: `${currentFragment.id}/pipeline:${number}`,
                fragmentId: currentFragment.id,
                number,
                instanceNum: Number(pipelineMatch[2]),
                nodeIds: [],
            };
            pipelines.push(currentPipeline);
            currentFragment.pipelineIds.push(currentPipeline.id);
            if (pipelines.length > MAX_PIPELINES) fail('DAG_TOO_LARGE', 'The Profile contains too many Pipelines.');
            currentNode = null;
            section = null;
            continue;
        }

        const operatorMatch = line.match(OPERATOR_RE);
        if (operatorMatch && currentFragment && currentPipeline) {
            const operatorType = operatorMatch[1];
            const spec = OPERATOR_SPECS[operatorType];
            const attributes = parseAttributes(operatorMatch[2]);
            const ordinal = currentPipeline.nodeIds.length;
            const id = `${currentPipeline.id}/operator:${ordinal}`;
            const table = operatorMatch[2].match(TABLE_NAME_RE)?.[1]?.trim();
            currentNode = {
                id,
                fragmentId: currentFragment.id,
                pipelineId: currentPipeline.id,
                ordinal,
                operatorType,
                operatorFamily: spec?.[0] ?? 'UNKNOWN',
                role: spec?.[1] ?? 'UNKNOWN',
                label: spec?.[2] ?? operatorType.replace(/_OPERATOR$/, '').replace(/_/g, ' '),
                planNodeId: attributes.id?.[0] ?? null,
                nereidsId: attributes.nereids_id?.[0] ?? null,
                destId: attributes.dest_id?.[0] ?? null,
                destIds: attributes.dest_id ?? [],
                known: Boolean(spec),
                lineNumber: index + 1,
                headerAttributes: attributes,
                planInfo: table ? { table } : {},
                timing: {},
                metrics: {},
                analysis: { heat: null, waitHeat: null, isBottleneck: false },
                counters: {},
            };
            parsedNodes.push(currentNode);
            currentPipeline.nodeIds.push(id);
            currentFragment.nodeIds.push(id);
            if (!spec) warnings.push({ kind: 'UNKNOWN_OPERATOR', nodeId: id, operatorType });
            if (parsedNodes.length > MAX_DAG_NODES) fail('DAG_TOO_LARGE', 'The execution graph contains too many nodes.');
            section = null;
            continue;
        }
        if (!currentNode) continue;
        if (PLANINFO_RE.test(line)) {
            section = 'plan';
            continue;
        }
        if (COUNTERS_RE.test(line)) {
            section = 'counters';
            continue;
        }
        if (section === 'counters') {
            const counterMatch = line.match(COUNTER_RE);
            if (counterMatch) currentNode.counters[counterMatch[1]] = parseCounter(counterMatch[1], counterMatch[2]);
            continue;
        }
        if (section === 'plan') {
            const planMatch = line.match(PLAN_KV_RE);
            if (!planMatch) continue;
            const outputKey = PLAN_INFO_WHITELIST[planMatch[1].trim().toLowerCase()];
            if (outputKey && currentNode.planInfo[outputKey] === undefined) {
                currentNode.planInfo[outputKey] = planMatch[2].trim().slice(0, MAX_PLAN_INFO_VALUE_LENGTH);
            }
        }
    }

    if (!inMerged) fail('DAG_UNAVAILABLE', 'The Profile does not contain a MergedProfile section.');
    if (parsedNodes.length === 0) {
        fail(
            'DAG_PARSE_FAILED',
            'The MergedProfile does not contain a compatible Fragment, Pipeline, and Operator structure.',
        );
    }
    const nodes = parsedNodes.map(finalizeNode);
    const edges: ProfileDagEdge[] = [];
    const unresolvedReferences: ProfileDagUnresolvedReference[] = [];

    const addEdge = (
        kind: DagEdgeKind,
        source: string,
        target: string,
        relationId: number | null = null,
        metadata: ProfileDagEdge['metadata'] = {},
    ) => {
        edges.push({
            id: `edge:${edges.length}`,
            kind,
            source,
            target,
            relationId: relationId === null ? null : String(relationId),
            resolved: true,
            metadata,
        });
        if (edges.length > MAX_DAG_EDGES) fail('DAG_TOO_LARGE', 'The execution graph contains too many edges.');
    };
    const addUnresolved = (kind: string, relationId: number | null, sourceNodeId: string, count: number) => {
        unresolvedReferences.push({
            kind,
            relationId: relationId === null ? null : String(relationId),
            sourceNodeId,
            reason: count === 0 ? 'TARGET_NOT_FOUND' : 'AMBIGUOUS_TARGET',
        });
    };

    for (const pipeline of pipelines) {
        for (let index = pipeline.nodeIds.length - 1; index > 0; index -= 1) {
            addEdge('PIPELINE_DATA', pipeline.nodeIds[index], pipeline.nodeIds[index - 1], null, { pipelineId: pipeline.id });
        }
    }

    const receiversByPlanNode = new Map<number, ProfileDagNode[]>();
    for (const node of nodes.filter(candidate => candidate.operatorType === 'EXCHANGE_OPERATOR')) {
        const id = node.planNodeId;
        if (id === null || id === undefined) continue;
        const receivers = receiversByPlanNode.get(id);
        if (receivers) receivers.push(node);
        else receiversByPlanNode.set(id, [node]);
    }
    for (const sink of nodes.filter(candidate => candidate.operatorType === 'DATA_STREAM_SINK_OPERATOR')) {
        for (const destId of sink.destIds) {
            const targets = receiversByPlanNode.get(destId) ?? [];
            if (targets.length === 1) {
                addEdge('EXCHANGE', sink.id, targets[0].id, destId, { destId, crossFragment: sink.fragmentId !== targets[0].fragmentId });
            } else addUnresolved('EXCHANGE', destId, sink.id, targets.length);
        }
    }

    const semanticIndex = new Map<string, ProfileDagNode[]>();
    for (const node of nodes) {
        if (node.nereidsId !== null && node.nereidsId !== undefined) {
            const key = relationKey(node.fragmentId, node.operatorFamily, 'nereids', node.nereidsId);
            const indexed = semanticIndex.get(key);
            if (indexed) indexed.push(node);
            else semanticIndex.set(key, [node]);
        }
        if (node.planNodeId !== null && node.planNodeId !== undefined) {
            const key = relationKey(node.fragmentId, node.operatorFamily, 'plan', node.planNodeId);
            const indexed = semanticIndex.get(key);
            if (indexed) indexed.push(node);
            else semanticIndex.set(key, [node]);
        }
    }
    for (const [family, sourceRoles, targetRoles, kind] of PAIRING_RULES) {
        for (const source of nodes.filter(node => node.operatorFamily === family && sourceRoles.includes(node.role))) {
            let relationId: number | null = null;
            let candidates: ProfileDagNode[] = [];
            if (source.nereidsId !== null && source.nereidsId !== undefined) {
                relationId = source.nereidsId;
                candidates = semanticIndex.get(relationKey(source.fragmentId, family, 'nereids', relationId)) ?? [];
            }
            candidates = candidates.filter(node => node.id !== source.id && targetRoles.includes(node.role));
            if (candidates.length === 0 && source.planNodeId !== null && source.planNodeId !== undefined) {
                relationId = source.planNodeId;
                candidates = (semanticIndex.get(relationKey(source.fragmentId, family, 'plan', relationId)) ?? []).filter(
                    node => node.id !== source.id && targetRoles.includes(node.role),
                );
            }
            if (candidates.length === 1) addEdge(kind, source.id, candidates[0].id, relationId);
            else addUnresolved(kind, relationId, source.id, candidates.length);
        }
    }

    const multicastConsumers = new Map<number, ProfileDagNode[]>();
    for (const node of nodes.filter(candidate => candidate.operatorType === 'MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR')) {
        const id = node.planNodeId;
        if (id === null || id === undefined) continue;
        const consumers = multicastConsumers.get(id);
        if (consumers) consumers.push(node);
        else multicastConsumers.set(id, [node]);
    }
    for (const producer of nodes.filter(candidate => candidate.operatorType === 'MULTI_CAST_DATA_STREAM_SINK_OPERATOR')) {
        producer.destIds.forEach((destId, branchIndex) => {
            const targets = multicastConsumers.get(destId) ?? [];
            if (targets.length === 1) {
                addEdge('MULTICAST', producer.id, targets[0].id, destId, {
                    destId,
                    branchIndex,
                    crossFragment: producer.fragmentId !== targets[0].fragmentId,
                });
            } else addUnresolved('MULTICAST', destId, producer.id, targets.length);
        });
    }

    validateGraph(nodes, edges);
    const performance = addPerformanceAnalysis(nodes);
    return {
        schemaVersion: '1.0',
        parserVersion: PROFILE_PARSER_VERSION,
        profile: {},
        graph: { direction: 'BOTTOM_TO_TOP', nodes, edges },
        fragments,
        pipelines,
        unresolvedReferences,
        warnings,
        summary: {
            fragmentCount: fragments.length,
            pipelineCount: pipelines.length,
            nodeCount: nodes.length,
            edgeCount: edges.length,
            unresolvedEdgeCount: unresolvedReferences.length,
            ...performance,
        },
    };
}
