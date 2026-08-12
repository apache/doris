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

import type { UiMe } from '../api/types';
import { HomePage } from './HomePage';

const admin: UiMe = {
  user: 'root',
  capabilities: ['NODE_STATUS_VIEW'],
  csrfToken: 'csrf',
};

function json(data: unknown) {
  return new Response(JSON.stringify({ data, requestId: 'req-home-page' }), {
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-home-page' },
  });
}

function UserOutlet({ me }: { me: UiMe }) {
  return <Outlet context={me} />;
}

function renderHome(me = admin) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[{ pathname: '/home', state: { emptyPassword: true } }]}>
        <Routes>
          <Route element={<UserOutlet me={me} />}>
            <Route path="/home" element={<HomePage />} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('HomePage', () => {
  it('shows real Version fields and every dynamic frontend field', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const path = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      if (path.endsWith('/home/version')) {
        return Promise.resolve(json({ version: '4.0.6', git: 'abc123', buildInfo: 'build-host', buildTime: 'today', features: 'avx2' }));
      }
      if (path.endsWith('/nodes/frontends')) {
        return Promise.resolve(json({
          columnNames: ['Name', 'Alive', 'FutureMetric'],
          rows: [['fe-1', 'true', 'future-value'], ['fe-2', 'false']],
        }));
      }
      if (path.endsWith('/nodes/backends')) return Promise.resolve(json({ columnNames: ['BackendId'], rows: [] }));
      return Promise.reject(new Error(`Unexpected request: ${path}`));
    });

    renderHome();

    expect(screen.getByText(/signed in with an empty password/i)).toBeInTheDocument();
    expect(await screen.findByText('4.0.6')).toBeInTheDocument();
    expect((await screen.findAllByText('FutureMetric')).length).toBeGreaterThan(0);
    expect(screen.getAllByText('future-value').length).toBeGreaterThan(0);
    expect(screen.getAllByText('—').length).toBeGreaterThan(0);

    fireEvent.change(screen.getByLabelText('Search frontends'), { target: { value: 'fe-2' } });
    await waitFor(() => expect(screen.queryAllByText('future-value')).toHaveLength(0));
    fireEvent.click(screen.getAllByText('fe-2')[0]);
    expect(await screen.findByText('Frontend details')).toBeInTheDocument();
    expect(screen.getAllByText('FutureMetric').length).toBeGreaterThan(1);
  });

  it('does not request node data without NODE_STATUS_VIEW', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      json({ version: '4.0.6', git: 'abc123', buildInfo: 'builder', buildTime: 'today', features: '' }),
    );

    renderHome({ user: 'analyst', capabilities: [], csrfToken: 'csrf' });

    expect(await screen.findByText('Node status is restricted for this account.')).toBeInTheDocument();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(fetchSpy).toHaveBeenCalledWith('/rest/v1/ui/home/version', expect.anything());
  });
});
