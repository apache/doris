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

import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Checkbox, Descriptions, Drawer, Input, message, Modal, Popconfirm, Table, Tabs, Tag, Tooltip } from 'antd';
import type { TableColumnsType } from 'antd';
import { useMemo, useState } from 'react';

import {
  type ConfigurationRow,
  type ConfigurationScope,
  type ConfigurationUpdateFailure,
  updateConfiguration,
  useConfiguration,
} from '../api/configuration';
import { OperationState } from '../components/operations/OperationState';

function includes(value: string, filter: string): boolean {
  return value.toLocaleLowerCase().includes(filter.trim().toLocaleLowerCase());
}

function compare(left: string, right: string): number {
  return left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' });
}

export function ConfigurationPage() {
  const [scope, setScope] = useState<ConfigurationScope>('fe');
  const [nameFilter, setNameFilter] = useState('');
  const [nodeFilter, setNodeFilter] = useState('');
  const [valueFilter, setValueFilter] = useState('');
  const [mutableOnly, setMutableOnly] = useState(false);
  const [selected, setSelected] = useState<ConfigurationRow | null>(null);
  const [editing, setEditing] = useState<ConfigurationRow | null>(null);
  const [editValue, setEditValue] = useState('');
  const [persist, setPersist] = useState(false);
  const [allNodes, setAllNodes] = useState(false);
  const [updateResult, setUpdateResult] = useState<{
    targetCount: number;
    failures: ConfigurationUpdateFailure[];
  } | null>(null);
  const [messageApi, messageContext] = message.useMessage();
  const query = useConfiguration(scope);

  const rows = useMemo(() => (query.data ?? []).filter((row) => (
    includes(row.name, nameFilter)
    && includes(row.node, nodeFilter)
    && includes(row.currentValue, valueFilter)
    && (!mutableOnly || row.mutable)
  )), [mutableOnly, nameFilter, nodeFilter, query.data, valueFilter]);

  const editableNodes = useMemo(() => editing === null ? [] : Array.from(new Set(
    (query.data ?? [])
      .filter((row) => row.name === editing.name && row.mutable)
      .map((row) => row.node),
  )), [editing, query.data]);
  const targetNodes = allNodes ? editableNodes : editing ? [editing.node] : [];

  const closeEditor = () => {
    setEditing(null);
    setEditValue('');
    setPersist(false);
    setAllNodes(false);
    setUpdateResult(null);
  };

  const updateMutation = useMutation({
    mutationFn: updateConfiguration,
    onSuccess: (result, variables) => {
      void query.refetch();
      if (result.failures.length === 0) {
        void messageApi.success(`Updated ${variables.name} on ${variables.nodes.length} node${variables.nodes.length === 1 ? '' : 's'}.`);
        closeEditor();
        return;
      }
      setUpdateResult({ targetCount: variables.nodes.length, failures: result.failures });
    },
  });

  const openEditor = (row: ConfigurationRow) => {
    if (!row.mutable) return;
    setEditing(row);
    setEditValue(row.currentValue);
    setPersist(false);
    setAllNodes(false);
    setUpdateResult(null);
    updateMutation.reset();
  };

  const columns: TableColumnsType<ConfigurationRow> = [
    {
      title: 'Name', dataIndex: 'name', key: 'name', width: 300,
      sorter: (left, right) => compare(left.name, right.name),
      render: (value: string) => <code className="configuration-name" title={value}>{value || '—'}</code>,
    },
    {
      title: 'Node', dataIndex: 'node', key: 'node', width: 190,
      sorter: (left, right) => compare(left.node, right.node),
      render: (value: string) => <span className="dynamic-cell" title={value}>{value || '—'}</span>,
    },
    {
      title: 'Node Type', dataIndex: 'nodeType', key: 'nodeType', width: 110,
      sorter: (left, right) => compare(left.nodeType, right.nodeType),
    },
    {
      title: 'Value Type', dataIndex: 'valueType', key: 'valueType', width: 130,
      sorter: (left, right) => compare(left.valueType, right.valueType),
    },
    {
      title: 'Master Only', dataIndex: 'masterOnly', key: 'masterOnly', width: 120,
      sorter: (left, right) => Number(left.masterOnly) - Number(right.masterOnly),
      render: (value: boolean | null) => value === null ? '—' : <Tag>{value ? 'Yes' : 'No'}</Tag>,
    },
    {
      title: 'Current Value', dataIndex: 'currentValue', key: 'currentValue', width: 240,
      sorter: (left, right) => compare(left.currentValue, right.currentValue),
      render: (value: string) => (
        <Tooltip title={value || '—'} placement="topLeft" overlayClassName="configuration-value-tooltip">
          <code className="configuration-value">{value || '—'}</code>
        </Tooltip>
      ),
    },
    {
      title: 'Mutable', dataIndex: 'mutable', key: 'mutable', width: 105,
      sorter: (left, right) => Number(left.mutable) - Number(right.mutable),
      render: (value: boolean) => <Tag color={value ? 'green' : 'default'}>{value ? 'Yes' : 'No'}</Tag>,
    },
    {
      title: 'Actions', key: 'actions', width: 90, fixed: 'right',
      render: (_value, row) => row.mutable ? (
        <Button
          size="small"
          aria-label={`Edit ${row.name} on ${row.node}`}
          onClick={(event) => {
            event.stopPropagation();
            openEditor(row);
          }}
        >Edit</Button>
      ) : null,
    },
  ];

  const clearFilters = () => {
    setNameFilter('');
    setNodeFilter('');
    setValueFilter('');
    setMutableOnly(false);
  };

  return (
    <main className="module-page operations-page configuration-page">
      {messageContext}
      <header className="page-heading">
        <div><p className="ui-label">Runtime settings</p><h1>Configuration</h1></div>
        <p>Inspect and update mutable frontend and backend configuration values across nodes.</p>
      </header>
      <section className="operations-section" aria-labelledby="configuration-heading">
        <div className="section-heading configuration-heading">
          <div><p className="ui-label">Cluster configuration</p><h2 id="configuration-heading">{scope.toUpperCase()} settings</h2></div>
          <span>{rows.length} of {query.data?.length ?? 0} rows</span>
        </div>
        <Tabs
          className="configuration-tabs"
          activeKey={scope}
          onChange={(key) => {
            setScope(key as ConfigurationScope);
            setSelected(null);
          }}
          items={[{ key: 'fe', label: 'Frontend' }, { key: 'be', label: 'Backend' }]}
        />
        <div className="configuration-filters" aria-label="Configuration filters">
          <Input allowClear aria-label="Filter by name" placeholder="Filter name" value={nameFilter} onChange={(event) => setNameFilter(event.target.value)} />
          <Input allowClear aria-label="Filter by node" placeholder="Filter node" value={nodeFilter} onChange={(event) => setNodeFilter(event.target.value)} />
          <Input allowClear aria-label="Filter by value" placeholder="Filter current value" value={valueFilter} onChange={(event) => setValueFilter(event.target.value)} />
          <Checkbox checked={mutableOnly} onChange={(event) => setMutableOnly(event.target.checked)}>Mutable only</Checkbox>
          <Button onClick={clearFilters}>Clear filters</Button>
          <Button loading={query.isFetching} onClick={() => { void query.refetch(); }}>Refresh</Button>
          <span className="last-refreshed">
            {query.dataUpdatedAt > 0 ? `Updated ${new Date(query.dataUpdatedAt).toLocaleTimeString()}` : 'Not refreshed'}
          </span>
        </div>
        <OperationState
          loading={query.isPending}
          error={query.error}
          hasData={Boolean(query.data)}
          onRetry={() => { void query.refetch(); }}
        />
        {query.data && (
          <Table<ConfigurationRow>
            className="operations-table configuration-table"
            columns={columns}
            dataSource={rows}
            rowKey="key"
            loading={query.isFetching}
            size="small"
            tableLayout="fixed"
            scroll={{ x: 1380 }}
            pagination={{
              defaultPageSize: 30,
              showSizeChanger: true,
              pageSizeOptions: [10, 20, 30, 50, 100],
              showTotal: (total, range) => `${range[0]}–${range[1]} of ${total}`,
            }}
            onRow={(row) => ({
              className: 'configuration-row',
              tabIndex: 0,
              onClick: () => setSelected(row),
              onKeyDown: (event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  setSelected(row);
                }
              },
            })}
          />
        )}
      </section>
      <Drawer
        className="configuration-details"
        title="Configuration details"
        size="large"
        open={selected !== null}
        onClose={() => setSelected(null)}
      >
        {selected && (
          <Descriptions bordered column={1} size="small">
            <Descriptions.Item label="Name"><code>{selected.name || '—'}</code></Descriptions.Item>
            <Descriptions.Item label="Node"><code>{selected.node || '—'}</code></Descriptions.Item>
            <Descriptions.Item label="Node Type">{selected.nodeType || '—'}</Descriptions.Item>
            <Descriptions.Item label="Value Type">{selected.valueType || '—'}</Descriptions.Item>
            <Descriptions.Item label="Master Only">{selected.masterOnly === null ? 'Not applicable' : selected.masterOnly ? 'Yes' : 'No'}</Descriptions.Item>
            <Descriptions.Item label="Current Value"><code className="configuration-detail-value">{selected.currentValue || '—'}</code></Descriptions.Item>
            <Descriptions.Item label="Mutable">{selected.mutable ? 'Yes' : 'No'}</Descriptions.Item>
          </Descriptions>
        )}
      </Drawer>
      <Modal
        className="configuration-editor"
        title="Edit configuration"
        open={editing !== null}
        onCancel={closeEditor}
        footer={editing ? [
          <Button key="cancel" onClick={closeEditor}>Cancel</Button>,
          <Popconfirm
            key="apply"
            title="Apply this configuration change?"
            description={`This will update ${targetNodes.length} ${scope.toUpperCase()} node${targetNodes.length === 1 ? '' : 's'}.`}
            okText="Apply"
            cancelText="Go back"
            onConfirm={() => updateMutation.mutate({
              scope,
              name: editing.name,
              nodes: targetNodes,
              value: editValue,
              persist,
            })}
          >
            <Button type="primary" loading={updateMutation.isPending} disabled={targetNodes.length === 0}>
              Apply change
            </Button>
          </Popconfirm>,
        ] : null}
      >
        {editing && (
          <div className="configuration-editor__body">
            <Alert
              type="warning"
              showIcon
              title="Configuration changes can affect running workloads."
              description="The server will verify administrator access, mutability, value validity, and node availability before applying this change."
            />
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label="Name"><code>{editing.name}</code></Descriptions.Item>
              <Descriptions.Item label="Selected node"><code>{editing.node}</code></Descriptions.Item>
              <Descriptions.Item label="Value type">{editing.valueType || '—'}</Descriptions.Item>
              <Descriptions.Item label="Current value"><code className="configuration-detail-value">{editing.currentValue || '—'}</code></Descriptions.Item>
            </Descriptions>
            <label className="configuration-editor__field">
              <span>New value</span>
              <Input.TextArea
                aria-label="New value"
                autoSize={{ minRows: 2, maxRows: 8 }}
                value={editValue}
                onChange={(event) => {
                  setEditValue(event.target.value);
                  setUpdateResult(null);
                  updateMutation.reset();
                }}
              />
            </label>
            {editableNodes.length > 1 && (
              <Checkbox checked={allNodes} onChange={(event) => setAllNodes(event.target.checked)}>
                Apply to all {editableNodes.length} mutable {scope.toUpperCase()} nodes that expose this setting
              </Checkbox>
            )}
            <Checkbox checked={persist} onChange={(event) => setPersist(event.target.checked)}>
              Persist after node restart
            </Checkbox>
            {updateMutation.error && (
              <Alert type="error" showIcon title="The configuration change failed" description={updateMutation.error.message} />
            )}
            {updateResult && (
              <Alert
                type={updateResult.failures.length < updateResult.targetCount ? 'warning' : 'error'}
                showIcon
                title={`${updateResult.targetCount - updateResult.failures.length} of ${updateResult.targetCount} nodes updated`}
                description={(
                  <ul className="configuration-update-failures">
                    {updateResult.failures.map((failure, index) => (
                      <li key={`${failure.node}:${index}`}><code>{failure.node || 'Unknown node'}</code>: {failure.error}</li>
                    ))}
                  </ul>
                )}
              />
            )}
          </div>
        )}
      </Modal>
    </main>
  );
}
