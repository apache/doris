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

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';

import { setCsrfToken } from '../api/csrf';
import { ConfigurationPage } from './ConfigurationPage';

const longValue = 'a-very-long-configuration-value-that-must-not-expand-the-entire-table-column';

function response(type: 'fe' | 'be') {
  const data = type === 'fe' ? {
    column_names: ['配置项', '节点', '节点类型', '配置值类型', 'MasterOnly', '配置值', '可修改'],
    rows: [
      ['web_sql_max_result_bytes', 'fe-a:8030', 'FE', 'long', 'false', '10485760', 'false'],
      ['http_port', 'fe-a:8030', 'FE', 'int', 'false', '8030', 'true'],
      ['runtime_filter_type', 'fe-a:8030', 'FE', 'String', 'false', longValue, 'true'],
      ['runtime_filter_type', 'fe-b:8030', 'FE', 'String', 'false', '8', 'true'],
    ],
  } : {
    column_names: ['配置项', '节点', '节点类型', '配置值类型', '配置值', '可修改'],
    rows: [['be_port', 'be-a:8040', 'BE', 'int32_t', '9060', 'true']],
  };
  return new Response(JSON.stringify({ code: 0, msg: 'success', data }), {
    headers: { 'Content-Type': 'application/json' },
  });
}

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><ConfigurationPage /></QueryClientProvider>);
}

describe('M13 and M14 Configuration page', () => {
  afterEach(() => {
    setCsrfToken(null);
    vi.restoreAllMocks();
  });

  it('filters FE rows, exposes mutable state and offers no way to change a setting', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(() => Promise.resolve(response('fe')));
    const { container } = renderPage();

    await screen.findByText('web_sql_max_result_bytes');
    expect(screen.getByRole('columnheader', { name: 'Master Only' })).toBeInTheDocument();
    // The page is read-only: no Edit affordance, and no Actions column to host one.
    expect(screen.queryByRole('columnheader', { name: 'Actions' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Edit / })).not.toBeInTheDocument();
    expect(screen.getByText('This page is read-only')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Filter by name'), { target: { value: 'http_port' } });
    expect(screen.queryByText('web_sql_max_result_bytes')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
    fireEvent.click(screen.getByText('web_sql_max_result_bytes'));
    expect(await screen.findByText('Configuration details')).toBeInTheDocument();
    expect(screen.getAllByText('10485760')).not.toHaveLength(0);
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));

    fireEvent.click(screen.getByLabelText('Mutable only'));
    const table = container.querySelector('.configuration-table');
    expect(table).not.toBeNull();
    await waitFor(() => expect(within(table as HTMLElement).queryByText('web_sql_max_result_bytes')).not.toBeInTheDocument());
    expect(within(table as HTMLElement).getByText('http_port')).toBeInTheDocument();
  }, 15_000);

  it('truncates the Current Value cell and exposes the complete value on hover', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(() => Promise.resolve(response('fe')));
    renderPage();

    const value = await screen.findByText(longValue);
    expect(value).toHaveClass('configuration-value');
    fireEvent.mouseEnter(value);
    expect(await screen.findByRole('tooltip')).toHaveTextContent(longValue);
  }, 15_000);

  it('loads and displays the different BE schema', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      return Promise.resolve(response(url.includes('type=be') ? 'be' : 'fe'));
    });
    renderPage();
    await screen.findByText('web_sql_max_result_bytes');

    fireEvent.click(screen.getByRole('tab', { name: 'Backend' }));

    await screen.findByText('be_port');
    // The address shows up twice: as the node tab and in the table's Node column.
    expect(screen.getAllByText('be-a:8040').length).toBeGreaterThan(0);
    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v2/manager/node/configuration_info?type=be',
      expect.objectContaining({ method: 'POST' }),
    );
  }, 15_000);
});
