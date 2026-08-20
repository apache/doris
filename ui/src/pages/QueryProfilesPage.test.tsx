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
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { QueryProfilesPage } from './QueryProfilesPage';
import { copyProfileText, findProfileMatches } from './queryProfiles/profileTextSearch';

function json(body: unknown) {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

function renderPage(path: string, client = new QueryClient({
  defaultOptions: { queries: { retry: false } },
})) {
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes><Route path="/query-profiles/:profileId?" element={<QueryProfilesPage />} /></Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('Query Profiles page', () => {
  it('links a current-FE Profile ID to its details', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: {
        column_names: ['Profile ID', 'Task Type', 'Task State'],
        href_column: ['Profile ID'],
        rows: [
          {
            'Profile ID': 'profile-01',
            'Task Type': 'QUERY',
            'Task State': 'FINISHED',
            __hrefPaths: ['/query_profile/profile-01'],
          },
          {
            'Profile ID': 'load-01',
            'Task Type': 'LOAD',
            'Task State': 'FINISHED',
            __hrefPaths: ['/query_profile/load-01'],
          },
        ],
      },
    }));
    renderPage('/query-profiles');
    expect(await screen.findByRole('link', { name: 'profile-01' })).toHaveAttribute('href', '/query-profiles/profile-01');
    expect(screen.queryByText('load-01')).not.toBeInTheDocument();
    expect(screen.getByText(/this FE's in-memory ProfileManager/i)).toBeInTheDocument();
  });

  it('refetches retained profiles on entry even when a fresh empty list is cached', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: 15_000 } },
    });
    client.setQueryData(['query-profiles'], {
      columnNames: ['Profile ID', 'Task Type'],
      rows: [],
    });
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: {
        column_names: ['Profile ID', 'Task Type'],
        rows: [{ 'Profile ID': 'fresh-profile', 'Task Type': 'QUERY' }],
      },
    }));

    renderPage('/query-profiles', client);

    await screen.findByText('fresh-profile');
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it('shows raw text and selects successive search matches', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: 'Summary:\nExecTime: 1ms\nExecTime: 2ms\n',
    }));
    renderPage('/query-profiles/profile-01');
    expect(await screen.findByRole('link', { name: 'Back to query profiles' })).toHaveAttribute('href', '/query-profiles');
    const viewer = await screen.findByLabelText('Query Profile text');
    if (!(viewer instanceof HTMLTextAreaElement)) throw new Error('Expected the Profile text viewer to be a textarea.');
    fireEvent.change(screen.getByLabelText('Search profile text'), { target: { value: 'ExecTime' } });
    fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(viewer.selectionStart).toBe(9));
    expect(screen.getByText('1 of 2')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(viewer.selectionStart).toBe(23));
    expect(screen.getByText('2 of 2')).toBeInTheDocument();
  });

  it('finds non-overlapping matches case-insensitively', () => {
    expect(findProfileMatches('ExecTime EXECTIME exec', 'exectime')).toEqual([0, 9]);
    expect(findProfileMatches('anything', '  ')).toEqual([]);
  });

  it('falls back to a temporary textarea when Clipboard API is unavailable', async () => {
    const clipboard = navigator.clipboard;
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined });
    const copy = vi.fn().mockReturnValue(true);
    Object.defineProperty(document, 'execCommand', { configurable: true, value: copy });
    await expect(copyProfileText('exact profile text')).resolves.toBeUndefined();
    expect(copy).toHaveBeenCalledWith('copy');
    expect(document.querySelector('textarea')).toBeNull();
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: clipboard });
    Reflect.deleteProperty(document, 'execCommand');
  });
});
