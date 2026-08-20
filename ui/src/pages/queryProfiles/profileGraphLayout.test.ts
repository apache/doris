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

import type { ProfileDagNode, ProfileGraphIR } from './profileGraphTypes';
import {
  buildElkGraph,
  formatBytes,
  formatDurationNs,
  layoutProfileDag,
  selectSlowestOperators,
} from './profileGraphLayout';

function node(id: string, fragmentId: string, pipelineId: string, maxNs: number): ProfileDagNode {
  return {
    id,
    fragmentId,
    pipelineId,
    ordinal: 0,
    operatorType: 'OLAP_SCAN_OPERATOR',
    operatorFamily: 'SCAN',
    role: 'SOURCE',
    label: 'OLAP SCAN',
    planNodeId: 1,
    nereidsId: null,
    destId: null,
    destIds: [],
    known: true,
    lineNumber: 1,
    planInfo: {},
    timing: { execTime: { maxNs } },
    metrics: {},
    analysis: { heat: 1, waitHeat: 0, isBottleneck: false },
  };
}

function fixture(): ProfileGraphIR {
  const first = node('fragment:0/pipeline:0/operator:0', 'fragment:0', 'fragment:0/pipeline:0', 10);
  const second = node('fragment:1/pipeline:2/operator:0', 'fragment:1', 'fragment:1/pipeline:2', 20);
  return {
    schemaVersion: '1.0',
    parserVersion: '0.2.0-ts.1',
    profile: {},
    graph: {
      direction: 'BOTTOM_TO_TOP',
      nodes: [first, second],
      edges: [{
        id: 'edge:0',
        kind: 'EXCHANGE',
        source: first.id,
        target: second.id,
        relationId: '1',
        resolved: true,
        metadata: { crossFragment: true },
      }],
    },
    fragments: [
      { id: 'fragment:0', number: 0, pipelineIds: [first.pipelineId], nodeIds: [first.id] },
      { id: 'fragment:1', number: 1, pipelineIds: [second.pipelineId], nodeIds: [second.id] },
    ],
    pipelines: [
      { id: first.pipelineId, fragmentId: first.fragmentId, number: 0, instanceNum: 1, nodeIds: [first.id] },
      { id: second.pipelineId, fragmentId: second.fragmentId, number: 2, instanceNum: 8, nodeIds: [second.id] },
    ],
    unresolvedReferences: [],
    warnings: [],
    summary: {
      fragmentCount: 2,
      pipelineCount: 2,
      nodeCount: 2,
      edgeCount: 1,
      unresolvedEdgeCount: 0,
      criticalNodeId: second.id,
      maxExecTimeNs: 20,
      maxWaitTimeNs: null,
    },
  };
}

describe('Profile graph layout adapter', () => {
  it('builds bottom-to-top compound Fragment input for ELK', () => {
    const graph = buildElkGraph(fixture());
    expect(graph.layoutOptions?.['elk.direction']).toBe('UP');
    expect(graph.layoutOptions?.['elk.hierarchyHandling']).toBe('INCLUDE_CHILDREN');
    expect(graph.children.map((fragment) => fragment.id)).toEqual(['fragment:0', 'fragment:1']);
    expect(graph.edges).toEqual([{ id: 'edge:0', sources: ['fragment:0/pipeline:0/operator:0'], targets: ['fragment:1/pipeline:2/operator:0'] }]);
  });

  it('maps ELK output to fixed read-only React Flow nodes', async () => {
    const result = await layoutProfileDag(fixture(), {
      layout: (graph) => Promise.resolve({
        ...graph,
        children: graph.children.map((fragment, index) => ({
          ...fragment,
          x: index * 300,
          y: index * 200,
          width: 260,
          height: 180,
          children: fragment.children?.map((operator) => ({ ...operator, x: 20, y: 54 })),
        })),
      }),
    });
    const operator = result.nodes.find((item) => item.id === 'fragment:1/pipeline:2/operator:0');
    expect(operator).toMatchObject({ parentId: 'fragment:1', draggable: false, connectable: false, position: { x: 20, y: 54 } });
    expect(operator?.data).toMatchObject({ pipelineLabel: 'Pipeline 2', instanceNum: 8 });
    expect(result.edges[0]).toMatchObject({ animated: false, reconnectable: false, data: { crossFragment: true } });
  });

  it('ranks slow operators and formats metrics without confusing zero and unknown', () => {
    expect(selectSlowestOperators(fixture()).map((item) => item.execMaxNs)).toEqual([20, 10]);
    expect(formatDurationNs(null)).toBe('Unknown');
    expect(formatDurationNs(0)).toBe('0 ns');
    expect(formatBytes(1536)).toBe('1.5 KiB');
  });
});
