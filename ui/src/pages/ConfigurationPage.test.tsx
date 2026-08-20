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

  it('filters FE rows, exposes mutable state and never offers Edit for an immutable row', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(() => Promise.resolve(response('fe')));
    const { container } = renderPage();

    await screen.findByText('web_sql_max_result_bytes');
    expect(screen.getByRole('columnheader', { name: 'Master Only' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit web_sql_max_result_bytes on fe-a:8030' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit http_port on fe-a:8030' })).toBeInTheDocument();

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

  it('updates all mutable nodes through the existing endpoint and displays a partial failure', async () => {
    setCsrfToken('csrf-config-page');
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      if (url.includes('/set_config/fe')) {
        return Promise.resolve(new Response(JSON.stringify({
          code: 0,
          msg: 'success',
          data: { failed: [{ config_name: 'runtime_filter_type', value: '4', node: 'fe-b:8030', err_info: 'rejected' }] },
        }), { headers: { 'Content-Type': 'application/json' } }));
      }
      return Promise.resolve(response('fe'));
    });
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: 'Edit runtime_filter_type on fe-a:8030' }));
    fireEvent.change(screen.getByLabelText('New value'), { target: { value: '4' } });
    fireEvent.click(screen.getByText('Apply to all 2 mutable FE nodes that expose this setting'));
    fireEvent.click(screen.getByText('Persist after node restart'));
    fireEvent.click(screen.getByRole('button', { name: 'Apply change' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Apply' }));

    expect(await screen.findByText('1 of 2 nodes updated')).toBeInTheDocument();
    expect(screen.getByText(/rejected/)).toBeInTheDocument();
    const mutationCall = fetchSpy.mock.calls.find(([input]) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      return url.includes('/set_config/fe');
    });
    expect(mutationCall).toBeDefined();
    const headers = new Headers(mutationCall?.[1]?.headers);
    expect(headers.get('X-Doris-CSRF-Token')).toBe('csrf-config-page');
    const requestBody = mutationCall?.[1]?.body;
    if (typeof requestBody !== 'string') throw new Error('Expected a JSON request body.');
    expect(JSON.parse(requestBody)).toEqual({
      runtime_filter_type: { node: ['fe-a:8030', 'fe-b:8030'], value: '4', persist: true },
    });
  }, 20_000);

  it('loads and displays the different BE schema', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      return Promise.resolve(response(url.includes('type=be') ? 'be' : 'fe'));
    });
    renderPage();
    await screen.findByText('web_sql_max_result_bytes');

    fireEvent.click(screen.getByRole('tab', { name: 'Backend' }));

    await screen.findByText('be_port');
    expect(screen.getByText('be-a:8040')).toBeInTheDocument();
    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v2/manager/node/configuration_info?type=be',
      expect.objectContaining({ method: 'POST' }),
    );
  }, 15_000);
});
