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

import { fireEvent, render, screen, within } from '@testing-library/react';

import { ProfileGraph } from './ProfileGraph';
import type { ProfileDagNode, ProfileGraphIR } from './profileGraphTypes';

vi.mock('./profileGraphLayout', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./profileGraphLayout')>();
  return {
    ...actual,
    layoutProfileDag: vi.fn(() => new Promise(() => undefined)),
  };
});

function fixture(): ProfileGraphIR {
  const node: ProfileDagNode = {
    id: 'fragment:0/pipeline:0/operator:1',
    fragmentId: 'fragment:0',
    pipelineId: 'fragment:0/pipeline:0',
    ordinal: 1,
    operatorType: 'OLAP_SCAN_OPERATOR',
    operatorFamily: 'SCAN',
    role: 'SOURCE',
    label: 'OLAP SCAN',
    planNodeId: 7,
    nereidsId: null,
    destId: null,
    destIds: [],
    known: true,
    lineNumber: 12,
    planInfo: { table: 'store_sales' },
    timing: { execTime: { maxNs: 20, avgNs: 10 } },
    metrics: {},
    analysis: { heat: 1, waitHeat: 0, isBottleneck: true },
  };
  return {
    schemaVersion: '1.0',
    parserVersion: '0.2.0-ts.1',
    profile: {},
    graph: { direction: 'BOTTOM_TO_TOP', nodes: [node], edges: [] },
    fragments: [{ id: 'fragment:0', number: 0, pipelineIds: [node.pipelineId], nodeIds: [node.id] }],
    pipelines: [{ id: node.pipelineId, fragmentId: node.fragmentId, number: 0, instanceNum: 1, nodeIds: [node.id] }],
    unresolvedReferences: [],
    warnings: [],
    summary: {
      fragmentCount: 1,
      pipelineCount: 1,
      nodeCount: 1,
      edgeCount: 0,
      unresolvedEdgeCount: 0,
      criticalNodeId: node.id,
      maxExecTimeNs: 20,
      maxWaitTimeNs: null,
    },
  };
}

describe('Profile graph floating panels', () => {
  it('hides and restores the slowest panel and opens non-modal operator details', () => {
    render(<ProfileGraph state="ready" graph={fixture()} error={null} />);

    const slowest = screen.getByLabelText('Slowest operators');
    expect(within(slowest).getByText('fragment:0/pipeline:0/operator:1')).toBeInTheDocument();
    fireEvent.click(within(slowest).getByRole('button', { name: 'Hide' }));
    expect(screen.queryByLabelText('Slowest operators')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Show slowest' }));
    const restored = screen.getByLabelText('Slowest operators');
    fireEvent.click(within(restored).getByRole('button', { name: /OLAP SCAN/i }));

    const details = screen.getByLabelText('Operator details');
    expect(within(details).getAllByText('fragment:0/pipeline:0/operator:1').length).toBeGreaterThan(0);
    expect(within(details).getByText('store_sales')).toBeInTheDocument();
    fireEvent.click(within(details).getByRole('button', { name: 'Hide' }));
    expect(screen.queryByLabelText('Operator details')).not.toBeInTheDocument();
  });
});
