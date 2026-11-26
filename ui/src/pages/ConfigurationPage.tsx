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

import { Alert, Button, Checkbox, Descriptions, Drawer, Input, Table, Tabs, Tag, Tooltip } from 'antd';
import type { TableColumnsType } from 'antd';
import { useMemo, useState } from 'react';

import {
  type ConfigurationRow,
  type ConfigurationScope,
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
  const [selectedNode, setSelectedNode] = useState('');
  const [nameFilter, setNameFilter] = useState('');
  const [nodeFilter, setNodeFilter] = useState('');
  const [valueFilter, setValueFilter] = useState('');
  const [mutableOnly, setMutableOnly] = useState(false);
  const [selected, setSelected] = useState<ConfigurationRow | null>(null);
  const query = useConfiguration(scope);

  const nodes = useMemo(() => Array.from(new Set((query.data ?? [])
    .map((row) => row.node)
    .filter(Boolean)))
    .sort(compare), [query.data]);
  const activeNode = nodes.includes(selectedNode) ? selectedNode : (nodes[0] ?? '');

  const rows = useMemo(() => (query.data ?? []).filter((row) => (
    row.node === activeNode
    && includes(row.name, nameFilter)
    && includes(row.node, nodeFilter)
    && includes(row.currentValue, valueFilter)
    && (!mutableOnly || row.mutable)
  )), [activeNode, mutableOnly, nameFilter, nodeFilter, query.data, valueFilter]);

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
  ];

  const clearFilters = () => {
    setNameFilter('');
    setNodeFilter('');
    setValueFilter('');
    setMutableOnly(false);
  };

  return (
    <main className="module-page operations-page configuration-page">
      <header className="page-heading">
        <h1>Configuration</h1>
      </header>
      <Alert
        className="configuration-notice"
        type="info"
        showIcon
        message="This page is read-only"
        description={scope === 'fe'
          ? 'Frontend settings are reported by the FE serving this page; open another FE web port to read its settings. Change a mutable setting with ADMIN SET FRONTEND CONFIG.'
          : 'Backend settings are read from every backend in the cluster. Change a mutable setting with the backend configuration API.'}
      />
      <section className="operations-section" aria-labelledby="configuration-heading">
        <div className="section-heading configuration-heading">
          <div><p className="ui-label">Cluster configuration</p><h2 id="configuration-heading">{scope.toUpperCase()} settings</h2></div>
          <span>{rows.length} of {query.data?.filter((row) => row.node === activeNode).length ?? 0} rows</span>
        </div>
        <Tabs
          className="configuration-tabs"
          activeKey={scope}
          onChange={(key) => {
            setScope(key as ConfigurationScope);
            setSelected(null);
            setSelectedNode('');
          }}
          items={[{ key: 'fe', label: 'Frontend' }, { key: 'be', label: 'Backend' }]}
        />
        <Tabs
          className="configuration-node-tabs"
          activeKey={activeNode}
          onChange={(key) => {
            setSelectedNode(key);
            setSelected(null);
          }}
          items={nodes.map((node) => ({ key: node, label: node }))}
          tabBarExtraContent={nodes.length > 0 ? (
            <span className="configuration-node-count">{nodes.length} {scope.toUpperCase()} nodes</span>
          ) : null}
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
    </main>
  );
}
