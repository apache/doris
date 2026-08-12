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

import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom';

import { UiApiError } from '../api/client';
import type { UiMe, WebSqlExecutionResult } from '../api/types';
import { PlaygroundPage } from './PlaygroundPage';

const sessionMocks = vi.hoisted(() => ({
  execute: vi.fn(),
  cancel: vi.fn(),
  reset: vi.fn(),
  close: vi.fn(),
  open: vi.fn(),
}));

vi.mock('@uiw/react-codemirror', () => ({
  default: ({ value, onChange, 'aria-label': ariaLabel }: { value: string; onChange: (value: string, update: unknown) => void; 'aria-label': string }) => (
    <textarea
      aria-label={ariaLabel}
      value={value}
      onChange={(event) => onChange(event.target.value, { state: { selection: { main: { from: 0, to: 0 } } } })}
    />
  ),
}));

vi.mock('./playground/useWebSqlSession', () => ({
  useWebSqlSession: () => ({
    status: 'ready',
    sessionId: 'fe-hint.session-id',
    error: null,
    ...sessionMocks,
  }),
}));

const me: UiMe = { user: 'root', capabilities: ['PLAYGROUND_USE'], csrfToken: 'csrf' };

function emptyResult(overrides: Partial<WebSqlExecutionResult> = {}): WebSqlExecutionResult {
  return {
    columns: [],
    rows: [],
    affectedRows: 0,
    elapsedTimeMs: 3,
    queryId: null,
    warnings: [],
    catalog: 'internal',
    database: null,
    truncated: false,
    ...overrides,
  };
}

function namedRows(column: string, names: string[]): WebSqlExecutionResult {
  return emptyResult({
    columns: [{ name: column, type: 'VARCHAR' }],
    rows: names.map((name) => [name]),
  });
}

function catalogRows(names: string[]): WebSqlExecutionResult {
  return emptyResult({
    columns: [
      { name: 'CatalogId', type: 'BIGINT' },
      { name: 'CatalogName', type: 'VARCHAR' },
      { name: 'Type', type: 'VARCHAR' },
      { name: 'IsCurrent', type: 'BOOLEAN' },
    ],
    rows: names.map((name, index) => [index, name, 'internal', index === 0]),
  });
}

function schemaRows(): WebSqlExecutionResult {
  return emptyResult({
    columns: [
      { name: 'Field', type: 'VARCHAR' },
      { name: 'Type', type: 'VARCHAR' },
      { name: 'Null', type: 'VARCHAR' },
      { name: 'Key', type: 'VARCHAR' },
      { name: 'Default', type: 'VARCHAR' },
      { name: 'Extra', type: 'VARCHAR' },
    ],
    rows: [['ss_item_sk', 'BIGINT', 'YES', '', null, '']],
  });
}

function treeSwitcher(name: string): HTMLElement {
  const title = screen.getByText(name);
  const node = title.closest('.ant-tree-treenode');
  const switcher = node?.querySelector<HTMLElement>('.ant-tree-switcher');
  if (!switcher) throw new Error(`No tree switcher found for ${name}`);
  return switcher;
}

function Context({ children }: { children?: ReactNode }) {
  return children ?? <Outlet context={me} />;
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/playground']}>
      <Routes>
        <Route element={<Context />}>
          <Route path="/playground" element={<PlaygroundPage />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

describe('PlaygroundPage', () => {
  beforeEach(() => {
    sessionMocks.execute.mockReset();
    sessionMocks.cancel.mockReset().mockResolvedValue({ cancelRequested: true });
    sessionMocks.reset.mockReset().mockResolvedValue({ sessionId: 'fe-hint.session-id' });
    sessionMocks.close.mockReset().mockResolvedValue({ closed: true });
    sessionMocks.open.mockReset().mockResolvedValue('fe-hint.new-session');
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (/^(SHOW|DESC)/.test(statement)) return Promise.resolve(emptyResult());
      return Promise.resolve(emptyResult());
    });
  });

  it('keeps multiple result tabs and displays rows, query metadata, empty results, and truncation', async () => {
    const queryResults = [
      emptyResult({
        columns: [{ name: 'answer', type: 'BIGINT' }],
        rows: [[42]],
        elapsedTimeMs: 8,
        queryId: 'query-42',
      }),
      emptyResult({ elapsedTimeMs: 2, queryId: 'query-empty', truncated: true }),
    ];
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (/^(SHOW|DESC)/.test(statement)) return Promise.resolve(emptyResult());
      return Promise.resolve(queryResults.shift()!);
    });

    renderPage();
    const sessionDetails = screen.getByLabelText('SQL session details');
    expect(sessionDetails).toHaveTextContent('Connection status');
    expect(sessionDetails).toHaveTextContent('ready');
    expect(sessionDetails).toHaveTextContent('Session ID');
    expect(sessionDetails).toHaveTextContent('fe-hint.session-id');
    const run = screen.getByRole('button', { name: /run selection/i });
    fireEvent.click(run);
    expect(await screen.findByText('Result 1')).toBeInTheDocument();
    expect(await screen.findByText('query-42')).toBeInTheDocument();
    expect(await screen.findByText('42')).toBeInTheDocument();

    fireEvent.click(run);
    expect(await screen.findByText('Result 2')).toBeInTheDocument();
    expect(screen.getByText('Result 1')).toBeInTheDocument();
    expect(await screen.findByText('Truncated')).toBeInTheDocument();
    expect(screen.getByText('The statement completed without a result set.')).toBeInTheDocument();
  });

  it('shows execution errors in Messages and wires Reset and Close', async () => {
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (/^(SHOW|DESC)/.test(statement)) return Promise.resolve(emptyResult());
      return Promise.reject(new UiApiError(400, {
        code: 'WEB_SQL_QUERY_ERROR',
        message: 'The SQL statement could not be executed.',
        requestId: 'req-error',
        details: 'Unknown column',
      }));
    });

    renderPage();
    fireEvent.click(screen.getByRole('button', { name: /run selection/i }));
    expect(await screen.findByText(/Unknown column/)).toBeInTheDocument();
    expect(screen.getByText(/WEB_SQL_QUERY_ERROR/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Reset connection' }));
    await waitFor(() => expect(sessionMocks.reset).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Close session' }));
    await waitFor(() => expect(sessionMocks.close).toHaveBeenCalledTimes(1));
  });

  it('loads database roots first, lazily loads tables, then loads and uses table structure', async () => {
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (statement === 'SHOW CATALOGS') return Promise.resolve(catalogRows(['internal']));
      if (statement === 'SHOW DATABASES FROM `internal`') return Promise.resolve(namedRows('Database', ['tpcds', 'demo']));
      if (statement === 'SHOW TABLES FROM `internal`.`tpcds`') return Promise.resolve(namedRows('Tables_in_tpcds', ['store_sales']));
      if (statement === 'DESC `internal`.`tpcds`.`store_sales`') return Promise.resolve(schemaRows());
      return Promise.resolve(emptyResult());
    });

    renderPage();
    expect(await screen.findByText('tpcds')).toBeInTheDocument();
    expect(sessionMocks.execute).not.toHaveBeenCalledWith(expect.stringContaining('SHOW TABLES'));

    fireEvent.click(treeSwitcher('tpcds'));
    expect(await screen.findByText('store_sales')).toBeInTheDocument();
    expect(sessionMocks.execute).toHaveBeenCalledWith('SHOW TABLES FROM `internal`.`tpcds`');

    fireEvent.click(screen.getByText('store_sales'));
    expect(await screen.findByText('ss_item_sk')).toBeInTheDocument();
    expect(sessionMocks.execute).toHaveBeenCalledWith('DESC `internal`.`tpcds`.`store_sales`');

    fireEvent.click(screen.getByText('ss_item_sk'));
    expect(screen.getByLabelText('SQL editor')).toHaveValue('SELECT COUNT(*) AS row_count\nFROM tpcds.store_sales;`ss_item_sk`');
    fireEvent.click(screen.getByRole('button', { name: 'Query table' }));
    expect(screen.getByLabelText('SQL editor')).toHaveValue('SELECT *\nFROM `internal`.`tpcds`.`store_sales`\nLIMIT 100;');
  });

  it('filters only loaded metadata and refreshes databases without loading every table', async () => {
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (statement === 'SHOW CATALOGS') return Promise.resolve(catalogRows(['internal']));
      if (statement === 'SHOW DATABASES FROM `internal`') return Promise.resolve(namedRows('Database', ['tpcds', 'demo']));
      if (statement === 'SHOW TABLES FROM `internal`.`tpcds`') return Promise.resolve(namedRows('Tables_in_tpcds', ['catalog_page']));
      return Promise.resolve(emptyResult());
    });

    renderPage();
    await screen.findByText('tpcds');
    fireEvent.click(treeSwitcher('tpcds'));
    await screen.findByText('catalog_page');

    const search = screen.getByLabelText('Search databases and loaded tables');
    fireEvent.change(search, { target: { value: 'catalog' } });
    expect(screen.getByText('catalog_page')).toBeInTheDocument();
    expect(screen.queryByText('demo')).not.toBeInTheDocument();
    expect(sessionMocks.execute).not.toHaveBeenCalledWith('SHOW TABLES FROM `internal`.`demo`');

    fireEvent.keyDown(search, { key: 'Enter', code: 'Enter' });
    await waitFor(() => expect(sessionMocks.execute).toHaveBeenCalledTimes(4));
    expect(sessionMocks.execute).toHaveBeenLastCalledWith('SHOW DATABASES FROM `internal`');
    await waitFor(() => expect(screen.queryByText('catalog_page')).not.toBeInTheDocument());

    fireEvent.change(search, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: 'Refresh object explorer' }));
    await waitFor(() => expect(sessionMocks.execute).toHaveBeenCalledTimes(5));
    expect(sessionMocks.execute).toHaveBeenLastCalledWith('SHOW DATABASES FROM `internal`');
  });

  it('keeps lazily loaded tables attached to their database when requests finish out of order', async () => {
    let resolveTpcds!: (result: WebSqlExecutionResult) => void;
    let resolveDemo!: (result: WebSqlExecutionResult) => void;
    const tpcds = new Promise<WebSqlExecutionResult>((resolve) => { resolveTpcds = resolve; });
    const demo = new Promise<WebSqlExecutionResult>((resolve) => { resolveDemo = resolve; });
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (statement === 'SHOW CATALOGS') return Promise.resolve(catalogRows(['internal']));
      if (statement === 'SHOW DATABASES FROM `internal`') return Promise.resolve(namedRows('Database', ['tpcds', 'demo']));
      if (statement === 'SHOW TABLES FROM `internal`.`tpcds`') return tpcds;
      if (statement === 'SHOW TABLES FROM `internal`.`demo`') return demo;
      return Promise.resolve(emptyResult());
    });

    renderPage();
    await screen.findByText('tpcds');
    fireEvent.click(screen.getByText('tpcds'));
    fireEvent.click(screen.getByText('demo'));
    await waitFor(() => expect(sessionMocks.execute).toHaveBeenCalledWith('SHOW TABLES FROM `internal`.`demo`'));
    await act(async () => {
      resolveDemo(namedRows('Tables_in_demo', ['demo_table']));
      await Promise.resolve();
    });
    expect(await screen.findByText('demo_table')).toBeInTheDocument();
    await act(async () => {
      resolveTpcds(namedRows('Tables_in_tpcds', ['store_sales']));
      await Promise.resolve();
    });
    expect(await screen.findByText('store_sales')).toBeInTheDocument();

    fireEvent.click(screen.getByText('demo_table'));
    fireEvent.click(screen.getByText('store_sales'));
    expect(sessionMocks.execute).toHaveBeenCalledWith('DESC `internal`.`demo`.`demo_table`');
    expect(sessionMocks.execute).toHaveBeenCalledWith('DESC `internal`.`tpcds`.`store_sales`');
    expect(sessionMocks.execute).toHaveBeenCalledWith('SHOW TABLES FROM `internal`.`tpcds`');
    expect(sessionMocks.execute).toHaveBeenCalledWith('SHOW TABLES FROM `internal`.`demo`');
  });

  it('clears table cache when switching catalogs', async () => {
    sessionMocks.execute.mockImplementation((statement: string) => {
      if (statement === 'SHOW CATALOGS') return Promise.resolve(catalogRows(['internal', 'iceberg']));
      if (statement === 'SHOW DATABASES FROM `internal`') return Promise.resolve(namedRows('Database', ['tpcds']));
      if (statement === 'SHOW TABLES FROM `internal`.`tpcds`') return Promise.resolve(namedRows('Tables_in_tpcds', ['store_sales']));
      if (statement === 'SHOW DATABASES FROM `iceberg`') return Promise.resolve(namedRows('Database', ['lakehouse']));
      return Promise.resolve(emptyResult());
    });

    renderPage();
    await screen.findByText('tpcds');
    fireEvent.click(treeSwitcher('tpcds'));
    await screen.findByText('store_sales');

    fireEvent.mouseDown(screen.getByLabelText('Catalog'));
    fireEvent.click(await screen.findByText('iceberg · internal'));
    expect(await screen.findByText('lakehouse')).toBeInTheDocument();
    expect(screen.queryByText('store_sales')).not.toBeInTheDocument();
    expect(sessionMocks.execute).toHaveBeenCalledWith('SHOW DATABASES FROM `iceberg`');
  });
});
