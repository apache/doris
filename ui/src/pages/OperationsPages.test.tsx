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
import type { ReactNode } from 'react';
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom';

import type { UiMe } from '../api/types';
import { SessionsPage } from './SessionsPage';
import { SystemPage } from './SystemPage';

const me: UiMe = { user: 'root', csrfToken: 'csrf' };

function json(data: unknown) {
  return new Response(JSON.stringify({ code: 0, msg: 'success', data }), {
    status: 200,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-page' },
  });
}

function renderPage(path: string, page: ReactNode, client = new QueryClient({
  defaultOptions: { queries: { retry: false } },
})) {
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<Outlet context={me} />}>
            <Route path="*" element={page} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('M9 operational pages', () => {
  it('navigates from the System root to a linked child Proc path', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      if (url.includes('%2Fbackends')) {
        return Promise.resolve(json({ column_names: ['Host'], rows: [{ Host: '127.0.0.1' }], parent_url: '/rest/v1/system?path=/' }));
      }
      return Promise.resolve(json({
        column_names: ['name'],
        href_columns: ['name'],
        rows: [{ name: 'backends', __hrefPaths: ['/rest/v1/system?path=//backends'] }],
        parent_url: '/rest/v1/system',
      }));
    });
    renderPage('/system?path=/', <SystemPage />);

    fireEvent.click(await screen.findByRole('link', { name: 'backends' }));
    await screen.findByText('127.0.0.1');
    expect(screen.getByRole('heading', { name: '/backends' })).toBeInTheDocument();
    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v1/system?path=%2Fbackends',
      expect.objectContaining({ credentials: 'same-origin' }),
    );
  });

  it('shows Session columns, active count, filtering, and refresh', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      column_names: ['Id', 'User', 'Info'],
      rows: [
        { Id: '9223372036854775807', User: 'root', Info: 'select 1' },
        { Id: '2', User: 'analyst', Info: null },
      ],
    }));
    renderPage('/sessions', <SessionsPage />);

    await screen.findByText('9223372036854775807');
    expect(screen.getByText('Active sessions')).toBeInTheDocument();
    expect(screen.getAllByText('2')).toHaveLength(2);
    fireEvent.change(screen.getByRole('searchbox', { name: 'Filter table' }), { target: { value: 'analyst' } });
    expect(screen.queryByText('9223372036854775807')).not.toBeInTheDocument();
    expect(screen.getByText('analyst')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(2));
  });

  it('refetches Sessions on entry even when a fresh empty result is cached', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: 15_000 } },
    });
    client.setQueryData(['operations', 'sessions'], { columnNames: ['Id'], rows: [] });
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      column_names: ['Id', 'User'],
      rows: [{ Id: '7', User: 'root' }],
    }));

    renderPage('/sessions', <SessionsPage />, client);

    await screen.findByText('root');
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});
