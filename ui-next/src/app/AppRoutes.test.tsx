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

import { QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

import { AppRoutes } from './AppRoutes';
import { queryClient } from './queryClient';

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-route' },
  });
}

function renderRoutes(path: string) {
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[path]}>
        <AppRoutes />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => queryClient.clear());

describe('AppRoutes authentication', () => {
  it('renders the English login form and submits an empty password without persisting credentials', async () => {
    let meCalls = 0;
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      if (url === '/rest/v1/ui/login' && init?.method === 'POST') {
        return Promise.resolve(
          json({
            data: { user: 'root', capabilities: ['PLAYGROUND_USE', 'QUERY_PROFILE_VIEW_OWN'], csrfToken: 'csrf' },
            requestId: 'req-route',
          }),
        );
      }
      if (url === '/rest/v1/ui/me') {
        meCalls += 1;
        if (meCalls === 1) {
          return Promise.resolve(
            json({ code: 'UI_UNAUTHENTICATED', message: 'Authentication is required.', requestId: 'req-route' }, 401),
          );
        }
        return Promise.reject(new Error('The login response should avoid a second /me request'));
      }
      return Promise.reject(new Error(`Unexpected request: ${url}`));
    });

    renderRoutes('/login');
    const username = await screen.findByLabelText('Username');
    fireEvent.change(username, { target: { value: 'root' } });
    const signIn = screen.getByRole('button', { name: 'Sign in' });
    await waitFor(() => expect(signIn).toBeEnabled());
    fireEvent.click(signIn);

    await screen.findByRole('heading', { name: 'Home' });
    expect(screen.getByText(/signed in with an empty password/i)).toBeInTheDocument();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it('guards protected routes with /me and capability-filters the navigation', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      json({
        data: { user: 'analyst', capabilities: ['PLAYGROUND_USE', 'QUERY_PROFILE_VIEW_OWN'], csrfToken: 'csrf' },
        requestId: 'req-route',
      }),
    );

    renderRoutes('/home');

    await screen.findByRole('heading', { name: 'Home' });
    expect(screen.getAllByText('analyst')).toHaveLength(2);
    expect(screen.getByText('Playground')).toBeInTheDocument();
    expect(screen.queryByText('Configuration')).not.toBeInTheDocument();
  });

  it('returns an expired protected session to Login', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      json({ code: 'UI_UNAUTHENTICATED', message: 'Authentication is required.', requestId: 'req-route' }, 401),
    );

    renderRoutes('/home');

    await waitFor(() => expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument());
    expect(screen.getByText('Your session expired. Sign in again to continue.')).toBeInTheDocument();
  });
});
