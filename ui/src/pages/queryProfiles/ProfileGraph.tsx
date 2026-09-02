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

import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type NodeMouseHandler,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Button, Checkbox, Descriptions, Input, Space, Tag } from 'antd';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { DagUiState, ProfileDagNode, ProfileGraphIR } from './profileGraphTypes';
import { ProfileGraphEdge } from './ProfileGraphEdge';
import {
  formatBytes,
  formatCount,
  formatDurationNs,
  layoutProfileDag,
  OPERATOR_NODE_HEIGHT,
  OPERATOR_NODE_WIDTH,
  selectSlowestOperators,
  type ProfileFlowNode,
} from './profileGraphLayout';
import { ProfileGraphFragmentNode, ProfileGraphNode } from './ProfileGraphNode';

interface ProfileGraphProps {
  state: DagUiState;
  graph: ProfileGraphIR | null;
  error: string | null;
}

const nodeTypes = { profileOperator: ProfileGraphNode, profileFragment: ProfileGraphFragmentNode };
const edgeTypes = { profileElk: ProfileGraphEdge };

function ProfileGraphLegend() {
  return (
    <aside className="profile-graph-legend" aria-label="Execution graph legend">
      <span><i className="profile-graph-legend__line profile-graph-legend__line--data" />Data flow</span>
      <span>
        <i className="profile-graph-legend__line profile-graph-legend__line--dependency" />
        Execution dependency (prerequisite → dependent)
      </span>
      <span><i className="profile-graph-legend__heat" />Longer execution</span>
      <span><i className="profile-graph-legend__wait" />Longer wait</span>
    </aside>
  );
}

function valueText(value: unknown): string | null {
  if (typeof value === 'string' || typeof value === 'boolean') return String(value);
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (Array.isArray(value)) {
    const parts = value.map(valueText).filter((value): value is string => value !== null);
    return parts.length ? parts.join(', ') : null;
  }
  return null;
}

function OperatorDetails({ node }: { node: ProfileDagNode }) {
  const items = [
    ['Type', node.operatorType],
    ['Role', node.role],
    ['Fragment', node.fragmentId.replace('fragment:', 'Fragment ')],
    ['Pipeline', node.pipelineId.split('/').at(-1)?.replace('pipeline:', 'Pipeline ') ?? node.pipelineId],
    ['Plan node ID', node.planNodeId],
    ['Nereids ID', node.nereidsId],
    ['Execution max', formatDurationNs(node.timing?.execTime?.maxNs)],
    ['Execution average', formatDurationNs(node.timing?.execTime?.avgNs)],
    ['Wait max', formatDurationNs(node.timing?.waitTime?.maxNs)],
    ['Input rows', formatCount(node.metrics?.inputRows?.max)],
    ['Output rows', formatCount(node.metrics?.outputRows?.max)],
    ['Memory peak', formatBytes(node.metrics?.memoryPeakBytes?.max)],
  ].filter(([, value]) => value !== null && value !== undefined);
  return (
    <>
      <Descriptions bordered column={1} size="small" items={items.map(([label, children]) => ({ label, children }))} />
      {Object.keys(node.planInfo).length > 0 && (
        <section className="profile-plan-info">
          <h3>Plan information</h3>
          <Descriptions
            bordered
            column={1}
            size="small"
            items={Object.entries(node.planInfo).flatMap(([label, value]) => {
              const text = valueText(value);
              return text === null ? [] : [{ label, children: text }];
            })}
          />
        </section>
      )}
    </>
  );
}

function visibleGraph(graph: ProfileGraphIR, collapsed: Set<string>): ProfileGraphIR {
  if (collapsed.size === 0) return graph;
  const nodes = graph.graph.nodes.filter((node) => !collapsed.has(node.fragmentId));
  const nodeIds = new Set(nodes.map((node) => node.id));
  return {
    ...graph,
    graph: { ...graph.graph, nodes, edges: graph.graph.edges.filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target)) },
    fragments: graph.fragments.filter((fragment) => !collapsed.has(fragment.id)),
    pipelines: graph.pipelines.filter((pipeline) => !collapsed.has(pipeline.fragmentId)),
  };
}

function ProfileGraphCanvas({ graph }: { graph: ProfileGraphIR }) {
  const [nodes, setNodes] = useState<ProfileFlowNode[]>([]);
  const [edges, setEdges] = useState<Awaited<ReturnType<typeof layoutProfileDag>>['edges']>([]);
  const [selected, setSelected] = useState<ProfileDagNode | null>(null);
  const [hotspotsVisible, setHotspotsVisible] = useState(true);
  const [search, setSearch] = useState('');
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [layoutError, setLayoutError] = useState<string | null>(null);
  const canvas = useRef<HTMLDivElement>(null);
  const { fitView, getInternalNode, setCenter } = useReactFlow<ProfileFlowNode>();
  const filtered = useMemo(() => visibleGraph(graph, collapsed), [collapsed, graph]);

  useEffect(() => {
    let cancelled = false;
    void layoutProfileDag(filtered).then((layout) => {
      if (cancelled) return;
      setNodes(layout.nodes);
      setEdges(layout.edges);
      setLayoutError(null);
      requestAnimationFrame(() => void fitView({ padding: 0.12 }));
    }).catch(() => {
      if (!cancelled) {
        setNodes([]);
        setEdges([]);
        setLayoutError('The execution graph could not be laid out.');
      }
    });
    return () => { cancelled = true; };
  }, [filtered, fitView]);

  const focusNode = useCallback((nodeId: string) => {
    const internal = getInternalNode(nodeId);
    if (!internal) return;
    const { x, y } = internal.internals.positionAbsolute;
    const width = internal.measured.width ?? OPERATOR_NODE_WIDTH;
    const height = internal.measured.height ?? OPERATOR_NODE_HEIGHT;
    setNodes((current) => current.map((node) => ({ ...node, selected: node.id === nodeId })));
    void setCenter(x + width / 2, y + height / 2, { zoom: 1, duration: 400 });
  }, [getInternalNode, setCenter]);

  const matches = useMemo(() => {
    const needle = search.trim().toLocaleLowerCase();
    if (!needle) return [];
    return graph.graph.nodes.filter((node) => [
      node.label,
      node.operatorType,
      node.planNodeId,
      node.nereidsId,
      node.planInfo.table,
    ].some((value) => String(value ?? '').toLocaleLowerCase().includes(needle)));
  }, [graph.graph.nodes, search]);
  const hotspots = useMemo(() => selectSlowestOperators(graph), [graph]);
  const operatorsById = useMemo(
    () => new Map(graph.graph.nodes.map((node) => [node.id, node])),
    [graph.graph.nodes],
  );
  const onNodeClick = useMemo<NodeMouseHandler<ProfileFlowNode>>(() => (_event, flowNode) => {
    if (flowNode.data.kind !== 'operator') return;
    setSelected(flowNode.data.node);
    focusNode(flowNode.id);
  }, [focusNode]);

  const reset = () => {
    setSearch('');
    setCollapsed(new Set());
    setSelected(null);
    setNodes((current) => current.map((node) => ({ ...node, selected: false })));
    requestAnimationFrame(() => void fitView({ padding: 0.12, duration: 300 }));
  };

  return (
    <div className="profile-graph-workspace">
      <div className="profile-graph-toolbar">
        <Input.Search
          allowClear
          aria-label="Search graph operators"
          placeholder="Search operator, table, or plan ID"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          onSearch={() => { if (matches[0]) focusNode(matches[0].id); }}
        />
        <Button onClick={() => void fitView({ padding: 0.12, duration: 300 })}>Fit</Button>
        <Button onClick={reset}>Reset</Button>
      </div>
      {search.trim() && (
        <div className="profile-graph-results" aria-live="polite">
          <strong>{matches.length} matches</strong>
          {matches.slice(0, 12).map((node) => (
            <Button key={node.id} size="small" onClick={() => focusNode(node.id)}>{node.label}</Button>
          ))}
        </div>
      )}
      <div className="profile-fragment-controls" aria-label="Fragment visibility">
        {graph.fragments.map((fragment) => (
          <Checkbox
            key={fragment.id}
            checked={!collapsed.has(fragment.id)}
            onChange={(event) => setCollapsed((current) => {
              const next = new Set(current);
              if (event.target.checked) next.delete(fragment.id); else next.add(fragment.id);
              return next;
            })}
          >Fragment {fragment.number}</Checkbox>
        ))}
      </div>
      <div className="profile-graph-content">
        <div ref={canvas} className="profile-graph-canvas" aria-label="Profile execution graph">
          {layoutError ? <div className="profile-graph-state" role="alert">{layoutError}</div> : nodes.length === 0
            ? <div className="profile-graph-state">Laying out the execution graph…</div>
            : (
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                onNodeClick={onNodeClick}
                nodesDraggable={false}
                nodesConnectable={false}
                edgesReconnectable={false}
                deleteKeyCode={null}
                minZoom={0.08}
                maxZoom={1.8}
              >
                <Background gap={24} size={1} />
                <MiniMap pannable zoomable aria-label="Execution graph overview" />
                <Controls showInteractive={false} />
              </ReactFlow>
            )}
          {hotspotsVisible ? (
            <aside className="profile-floating-panel profile-hotspots" aria-label="Slowest operators">
              <div className="profile-floating-panel__heading">
                <p className="ui-label">Slowest by exec max</p>
                <Button type="text" size="small" onClick={() => setHotspotsVisible(false)}>Hide</Button>
              </div>
              {hotspots.length === 0 ? <p className="profile-floating-panel__empty">No execution timing available.</p> : hotspots.map((item, index) => (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => {
                    setSelected(operatorsById.get(item.id) ?? null);
                    focusNode(item.id);
                  }}
                >
                  <span className="profile-hotspots__rank">{index + 1}</span>
                  <strong>{item.label}</strong>
                  <small className="profile-hotspots__duration">{formatDurationNs(item.execMaxNs)}</small>
                  <small className="profile-hotspots__id">{item.id}</small>
                </button>
              ))}
            </aside>
          ) : (
            <Button className="profile-floating-panel-show profile-floating-panel-show--left" onClick={() => setHotspotsVisible(true)}>
              Show slowest
            </Button>
          )}
          {selected && (
            <aside className="profile-floating-panel profile-operator-details" aria-label="Operator details">
              <div className="profile-floating-panel__heading">
                <div>
                  <p className="ui-label">Operator details</p>
                  <h3>{selected.label}</h3>
                  <p className="profile-operator-details__id">{selected.id}</p>
                </div>
                <Button type="text" size="small" onClick={() => setSelected(null)}>Hide</Button>
              </div>
              <OperatorDetails node={selected} />
            </aside>
          )}
        </div>
      </div>
    </div>
  );
}

export function ProfileGraph({ state, graph, error }: ProfileGraphProps) {
  if (state !== 'ready' || graph === null) {
    const message = error ?? (state === 'parsing'
      ? 'Parsing the execution graph…'
      : 'Select Visual to build the execution graph.');
    return <div className="profile-graph-state" role={state === 'failed' || state === 'unavailable' ? 'alert' : 'status'}>{message}</div>;
  }
  const notices = graph.warnings.length + graph.unresolvedReferences.length;
  return (
    <section className="profile-graph" aria-label="Execution graph">
      <Space wrap className="profile-graph-summary">
        <Tag>{graph.summary.fragmentCount} fragments</Tag>
        <Tag>{graph.summary.pipelineCount} pipelines</Tag>
        <Tag>{graph.summary.nodeCount} operators</Tag>
        <Tag>{graph.summary.edgeCount} connections</Tag>
        {notices > 0 && <Tag color="warning">{notices} parsing notices</Tag>}
      </Space>
      <ProfileGraphLegend />
      <ReactFlowProvider><ProfileGraphCanvas graph={graph} /></ReactFlowProvider>
    </section>
  );
}
