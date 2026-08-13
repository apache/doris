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

import type { TableColumnsType } from 'antd';
import { Alert, Button, Checkbox, Descriptions, Drawer, Empty, Input, Popover, Skeleton, Table, Tabs, Tag } from 'antd';
import { useMemo, useState } from 'react';
import { useLocation } from 'react-router-dom';

import { UiApiError } from '../api/client';
import { useBackends, useFrontends, useVersion } from '../api/home';
import type { UiNodeTable } from '../api/types';
import { cellValue, filterNodeRecords, summarizeNodes, toNodeRecords, type NodeRecord } from './nodeTable';

interface NodeTablePanelProps {
  kind: 'Frontend' | 'Backend';
  table?: UiNodeTable;
  loading: boolean;
  error: Error | null;
  updatedAt: number;
  onRefresh: () => void;
}

function ErrorState({ error, title }: { error: Error; title: string }) {
  const requestId = error instanceof UiApiError ? error.requestId : 'unavailable';
  return (
    <Alert
      type="error"
      showIcon
      title={title}
      description={<span>{error.message} <strong>Request ID:</strong> {requestId}</span>}
    />
  );
}

function AliveValue({ value }: { value: string }) {
  const normalized = value.trim().toLocaleLowerCase();
  if (normalized === 'true') return <Tag color="success"><i className="status-dot" />Alive</Tag>;
  if (normalized === 'false') return <Tag color="error"><i className="status-dot" />Dead</Tag>;
  return <span>{value || '—'}</span>;
}

function NodeTablePanel({ kind, table, loading, error, updatedAt, onRefresh }: NodeTablePanelProps) {
  const [search, setSearch] = useState('');
  const [hiddenColumns, setHiddenColumns] = useState<string[]>([]);
  const [selected, setSelected] = useState<NodeRecord | null>(null);

  const visibleColumns = (table?.columnNames ?? []).filter((name) => !hiddenColumns.includes(name));

  const records = useMemo(() => filterNodeRecords(toNodeRecords(table ?? { columnNames: [], rows: [] }), search), [table, search]);
  const summary = useMemo(() => summarizeNodes(table ?? { columnNames: [], rows: [] }), [table]);
  const aliveIndex = table?.columnNames.findIndex((name) => name.toLocaleLowerCase() === 'alive') ?? -1;
  const columns = useMemo<TableColumnsType<NodeRecord>>(
    () => (table?.columnNames ?? [])
      .map((name, index) => ({ name, index }))
      .filter(({ name }) => visibleColumns.includes(name))
      .map(({ name, index }) => ({
        title: name,
        key: `${name}-${index}`,
        width: Math.max(136, Math.min(320, name.length * 12 + 48)),
        ellipsis: true,
        sorter: (left, right) => cellValue(left, index).localeCompare(cellValue(right, index), undefined, { numeric: true }),
        render: (_value, record) => index === aliveIndex
          ? <AliveValue value={cellValue(record, index)} />
          : <span className="table-cell-value" title={cellValue(record, index)}>{cellValue(record, index) || '—'}</span>,
      })),
    [aliveIndex, table, visibleColumns],
  );

  const columnPicker = (
    <div className="column-picker" aria-label={`${kind} columns`}>
      <Checkbox
        checked={visibleColumns.length === (table?.columnNames.length ?? 0)}
        indeterminate={visibleColumns.length > 0 && visibleColumns.length < (table?.columnNames.length ?? 0)}
        onChange={(event) => setHiddenColumns(event.target.checked ? [] : [...(table?.columnNames ?? [])])}
      >
        All columns
      </Checkbox>
      {(table?.columnNames ?? []).map((name, index) => (
        <Checkbox
          key={`${name}-${index}`}
          checked={visibleColumns.includes(name)}
          onChange={(event) => setHiddenColumns((current) => event.target.checked
            ? current.filter((column) => column !== name)
            : [...current, name])}
        >
          {name}
        </Checkbox>
      ))}
    </div>
  );

  return (
    <section className="node-panel" aria-label={`${kind} status`}>
      <div className="node-summary" aria-label={`${kind} summary`}>
        <div><span>Total</span><strong>{summary.total}</strong></div>
        <div><span>Alive</span><strong className="healthy-number">{summary.alive}</strong></div>
        <div><span>Dead</span><strong className={summary.dead > 0 ? 'danger-number' : ''}>{summary.dead}</strong></div>
        <div><span>Unknown</span><strong>{summary.unknown}</strong></div>
      </div>
      <div className="node-toolbar">
        <Input.Search
          allowClear
          aria-label={`Search ${kind.toLocaleLowerCase()}s`}
          placeholder={`Search all ${kind.toLocaleLowerCase()} fields`}
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
        <Popover content={columnPicker} trigger="click" placement="bottomRight">
          <Button disabled={!table?.columnNames.length}>Columns ({visibleColumns.length})</Button>
        </Popover>
        <Button loading={loading} onClick={onRefresh}>Refresh</Button>
        <span className="last-refreshed">
          {updatedAt > 0 ? `Updated ${new Date(updatedAt).toLocaleTimeString()}` : 'Not refreshed'}
        </span>
      </div>
      {error && <ErrorState error={error} title="Node status could not be loaded." />}
      {!error && loading && !table && <Skeleton active paragraph={{ rows: 5 }} />}
      {!error && table && table.rows.length === 0 && <Empty description={`No ${kind.toLocaleLowerCase()} nodes returned.`} />}
      {!error && table && table.rows.length > 0 && (
        <Table<NodeRecord>
          className="node-table"
          columns={columns}
          dataSource={records}
          loading={loading}
          pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: false }}
          scroll={{ x: 'max-content' }}
          size="small"
          onRow={(record) => ({ onClick: () => setSelected(record) })}
          locale={{ emptyText: search ? 'No nodes match this search.' : 'No nodes returned.' }}
          rowClassName={() => 'node-row'}
        />
      )}
      <Drawer
        title={`${kind} details`}
        open={selected !== null}
        size="large"
        onClose={() => setSelected(null)}
      >
        <Descriptions bordered column={1} size="small">
          {(table?.columnNames ?? []).map((name, index) => (
            <Descriptions.Item key={`${name}-${index}`} label={name}>
              {selected ? cellValue(selected, index) || '—' : '—'}
            </Descriptions.Item>
          ))}
        </Descriptions>
      </Drawer>
    </section>
  );
}

export function HomePage() {
  const location = useLocation();
  const version = useVersion();
  const frontends = useFrontends(true);
  const backends = useBackends(true);
  const emptyPassword = Boolean((location.state as { emptyPassword?: boolean } | null)?.emptyPassword);

  return (
    <main className="module-page home-page">
      <header className="page-heading">
        <div><p className="ui-label">Cluster overview</p><h1>Home</h1></div>
        <p>Build identity and current node state reported directly by this Doris frontend.</p>
      </header>
      {emptyPassword && (
        <Alert
          className="home-security-alert"
          type="warning"
          showIcon
          title="This account signed in with an empty password. Configure a password before using this console on an untrusted network."
        />
      )}
      <section className="version-section" aria-labelledby="version-heading">
        <div className="section-heading"><div><p className="ui-label">Build identity</p><h2 id="version-heading">Version</h2></div></div>
        {version.isPending && <Skeleton active paragraph={{ rows: 2 }} />}
        {version.error && <ErrorState error={version.error} title="Version information could not be loaded." />}
        {version.data && (
          <Descriptions className="version-grid" bordered column={{ xs: 1, sm: 1, md: 2, lg: 3 }} size="small">
            <Descriptions.Item label="Version">{version.data.version || '—'}</Descriptions.Item>
            <Descriptions.Item label="Git">{version.data.git || '—'}</Descriptions.Item>
            <Descriptions.Item label="Build Time">{version.data.buildTime || '—'}</Descriptions.Item>
            <Descriptions.Item label="Build Info">{version.data.buildInfo || '—'}</Descriptions.Item>
            <Descriptions.Item label="Features">{version.data.features || '—'}</Descriptions.Item>
          </Descriptions>
        )}
      </section>
      <section className="nodes-section" aria-labelledby="nodes-heading">
        <div className="section-heading"><div><p className="ui-label">Live topology</p><h2 id="nodes-heading">Nodes</h2></div></div>
        <Tabs
            defaultActiveKey="frontends"
            items={[
              {
                key: 'frontends',
                label: `Frontends (${frontends.data?.rows.length ?? 0})`,
                children: <NodeTablePanel kind="Frontend" table={frontends.data} loading={frontends.isFetching} error={frontends.error} updatedAt={frontends.dataUpdatedAt} onRefresh={() => { void frontends.refetch(); }} />,
              },
              {
                key: 'backends',
                label: `Backends (${backends.data?.rows.length ?? 0})`,
                children: <NodeTablePanel kind="Backend" table={backends.data} loading={backends.isFetching} error={backends.error} updatedAt={backends.dataUpdatedAt} onRefresh={() => { void backends.refetch(); }} />,
              },
            ]}
          />
      </section>
    </main>
  );
}
