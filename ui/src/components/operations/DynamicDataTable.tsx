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

import { Button, Empty, Input, Table } from 'antd';
import type { TableColumnsType } from 'antd';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';

import {
  buildClientTableView,
  displayCell,
  type DynamicCell,
  type DynamicRow,
  type DynamicTableData,
  type SortDirection,
} from './dynamicTable';

export interface DynamicCellRenderContext {
  value: DynamicCell;
  displayValue: string;
  row: DynamicRow;
  columnName: string;
  columnIndex: number;
}

interface DynamicDataTableProps {
  data: DynamicTableData;
  loading?: boolean;
  searchable?: boolean;
  searchPlaceholder?: string;
  pageSize?: number;
  onRefresh?: () => void;
  updatedAt?: number;
  renderCell?: (context: DynamicCellRenderContext) => ReactNode;
}

interface TableRecord extends DynamicRow {
  recordKey: string;
}

export function DynamicDataTable({
  data,
  loading = false,
  searchable = true,
  searchPlaceholder = 'Filter all columns',
  pageSize: initialPageSize = 30,
  onRefresh,
  updatedAt = 0,
  renderCell,
}: DynamicDataTableProps) {
  const [search, setSearch] = useState('');
  const [sortColumn, setSortColumn] = useState<number | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);

  const view = useMemo(
    () => buildClientTableView(data.rows, { search, sortColumn, sortDirection, page, pageSize }),
    [data.rows, page, pageSize, search, sortColumn, sortDirection],
  );

  const columns = useMemo<TableColumnsType<TableRecord>>(
    () => data.columnNames.map((columnName, columnIndex) => ({
      title: columnName || `Column ${columnIndex + 1}`,
      key: `${columnName}-${columnIndex}`,
      width: Math.max(132, Math.min(360, (columnName || '').length * 11 + 52)),
      sorter: true,
      sortOrder: sortColumn === columnIndex ? sortDirection : null,
      ellipsis: true,
      render: (_value, row) => {
        const value = row.cells[columnIndex];
        const rendered = renderCell?.({
          value,
          displayValue: displayCell(value),
          row,
          columnName,
          columnIndex,
        });
        if (rendered !== undefined) return rendered;
        const text = displayCell(value);
        return <span className="dynamic-cell" title={text === '—' ? undefined : text}>{text}</span>;
      },
    })),
    [data.columnNames, renderCell, sortColumn, sortDirection],
  );

  const records = view.rows.map((row) => ({ ...row, recordKey: row.key }));

  return (
    <div className="dynamic-data-table">
      <div className="operations-toolbar">
        {searchable && (
          <Input.Search
            allowClear
            aria-label="Filter table"
            placeholder={searchPlaceholder}
            value={search}
            onChange={(event) => {
              setSearch(event.target.value);
              setPage(1);
            }}
          />
        )}
        {onRefresh && <Button loading={loading} onClick={onRefresh}>Refresh</Button>}
        <span className="last-refreshed">
          {updatedAt > 0 ? `Updated ${new Date(updatedAt).toLocaleTimeString()}` : 'Not refreshed'}
        </span>
      </div>
      <Table<TableRecord>
        className="operations-table"
        columns={columns}
        dataSource={records}
        rowKey="recordKey"
        loading={loading}
        size="small"
        scroll={{ x: 'max-content' }}
        locale={{ emptyText: <Empty description={search ? 'No rows match this filter.' : 'No rows returned.'} /> }}
        pagination={{
          current: view.page,
          pageSize,
          total: view.total,
          showSizeChanger: true,
          pageSizeOptions: [10, 20, 30, 50, 100],
          showTotal: (total, range) => `${range[0]}–${range[1]} of ${total}`,
        }}
        onChange={(pagination, _filters, sorter) => {
          const selected = Array.isArray(sorter) ? sorter[0] : sorter;
          const key = typeof selected?.columnKey === 'string' ? selected.columnKey : '';
          const matchedIndex = data.columnNames.findIndex((name, index) => `${name}-${index}` === key);
          const nextColumn = selected?.order && matchedIndex >= 0 ? matchedIndex : null;
          const nextDirection = selected?.order ?? null;
          const nextPageSize = pagination.pageSize ?? initialPageSize;
          const queryChanged = nextColumn !== sortColumn
            || nextDirection !== sortDirection
            || nextPageSize !== pageSize;
          setSortColumn(nextColumn);
          setSortDirection(nextDirection);
          setPage(queryChanged ? 1 : (pagination.current ?? 1));
          setPageSize(nextPageSize);
        }}
      />
    </div>
  );
}
