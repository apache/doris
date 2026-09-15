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
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom';

import type { UiLogSnapshot, UiMe } from '../api/types';
import { LogPage } from './LogPage';

const me: UiMe = {
  user: 'root',
  csrfToken: 'csrf',
};

function snapshot(verboseNames: string[] = []): UiLogSnapshot {
  return {
    level: 'INFO',
    mode: 'ASYNC',
    verboseNames,
    auditNames: ['slow_query', 'query'],
    logPath: '/mnt/log/fe.warn.log',
    showingLastBytes: 42,
    contents: 'warning <script>alert(1)</script>\nnext line',
    contentError: null,
  };
}

function response(data: UiLogSnapshot) {
  return new Response(JSON.stringify({ code: 0, data: {
    LogConfiguration: {
      Level: data.level,
      Mode: data.mode,
      VerboseNames: data.verboseNames.join(','),
      AuditNames: data.auditNames.join(','),
    },
    LogContents: {
      logPath: data.logPath,
      showingLast: `${data.showingLastBytes} bytes of log`,
      log: `<pre>${data.contents.replace(/\n/g, '</br>')}</pre>`,
      ...(data.contentError ? { error: data.contentError } : {}),
    },
  } }), {
    status: 200,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-log-page' },
  });
}

function renderPage(currentUser: UiMe = me) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/log']}>
        <Routes>
          <Route element={<Outlet context={currentUser} />}>
            <Route path="/log" element={<LogPage />} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('LogPage', () => {
  it('renders configuration and treats log contents as plain text', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(response(snapshot()));
    const rendered = renderPage();

    await screen.findByRole('heading', { name: /^Log$/ });
    expect(await screen.findByText('INFO')).toBeInTheDocument();
    expect(screen.getByText('slow_query, query')).toBeInTheDocument();
    expect(screen.getByText('/mnt/log/fe.warn.log')).toBeInTheDocument();
    expect(screen.getByText(/warning <script>alert\(1\)<\/script>/)).toBeInTheDocument();
    expect(rendered.container.querySelector('script')).toBeNull();
    expect(screen.getByText('No verbose logger names configured.')).toBeInTheDocument();
  });

  it('adds and deletes a verbose logger with confirmation', async () => {
    const logger = 'org.apache.doris.ui.M10Probe';
    let verboseNames: string[] = [];
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((_input, init) => {
      if (init?.method === 'POST') {
        verboseNames = typeof init.body === 'string' && init.body.startsWith('add_verbose=') ? [logger] : [];
        return Promise.resolve(new Response(JSON.stringify({ code: 0, data: {} }), {
          headers: { 'Content-Type': 'application/json' },
        }));
      }
      return Promise.resolve(response(snapshot(verboseNames)));
    });
    renderPage();
    await screen.findByText('No verbose logger names configured.');

    fireEvent.change(screen.getByLabelText('New verbose logger name'), { target: { value: logger } });
    fireEvent.click(screen.getByRole('button', { name: 'Add verbose name' }));
    await screen.findByText(logger);
    expect(fetchSpy).toHaveBeenCalledWith('/rest/v1/log', expect.objectContaining({
      method: 'POST', body: `add_verbose=${logger}`,
    }));

    fireEvent.click(screen.getByRole('button', { name: `Delete ${logger}` }));
    fireEvent.click(await screen.findByRole('button', { name: /^Delete$/ }));
    await waitFor(() => expect(screen.getByText('No verbose logger names configured.')).toBeInTheDocument());
    expect(fetchSpy).toHaveBeenCalledWith('/rest/v1/log', expect.objectContaining({
      method: 'POST', body: `del_verbose=${logger}`,
    }));
  });

});
