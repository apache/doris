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

import { adaptLegacyTable, fetchSessions, fetchSystem, normalizeProcPath, procPathFromHref } from './operations';

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-ops' },
  });
}

describe('legacy operational API adapter', () => {
  it('preserves dynamic columns, duplicate names, links, empty cells, and large integers', () => {
    expect(adaptLegacyTable({
      column_names: ['Name', 'Name', 'ConnectionId', 'Empty'],
      href_columns: ['Name'],
      rows: [{
        Name: 'frontends',
        ConnectionId: '9223372036854775807',
        Empty: null,
        __hrefPaths: ['/rest/v1/system?path=//frontends'],
      }],
    })).toEqual({
      columnNames: ['Name', 'Name', 'ConnectionId', 'Empty'],
      rows: [{
        key: 'row-0',
        cells: ['frontends', 'frontends', '9223372036854775807', null],
        links: { 0: '/rest/v1/system?path=//frontends' },
      }],
    });
  });

  it('normalizes Proc paths and internal hrefs', () => {
    expect(normalizeProcPath('//catalogs///internal db/')).toBe('/catalogs/internal db');
    expect(procPathFromHref('/rest/v1/system?path=//backends')).toBe('/backends');
    expect(procPathFromHref('/rest/v1/session')).toBeNull();
  });

  it('URL-encodes a System path and adapts the result', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: { column_names: ['name'], rows: [{ name: 'tables' }], parent_url: '/rest/v1/system?path=//catalogs' },
    }));

    await expect(fetchSystem('/catalogs/internal db')).resolves.toMatchObject({
      parentPath: '/catalogs',
      table: { columnNames: ['name'], rows: [{ cells: ['tables'] }] },
    });
    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v1/system?path=%2Fcatalogs%2Finternal+db',
      expect.objectContaining({ method: 'GET', credentials: 'same-origin' }),
    );
  });

  it('keeps a Session connection ID as an exact string', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: { column_names: ['Id', 'User'], rows: [{ Id: '18446744073709551615', User: 'root' }] },
    }));
    await expect(fetchSessions()).resolves.toMatchObject({
      rows: [{ cells: ['18446744073709551615', 'root'] }],
    });
  });

  it.each([
    [403, 'UI_FORBIDDEN'],
    [500, 'UI_OPERATION_FAILED'],
  ])('normalizes an HTTP %s failure', async (status, code) => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({ code: 1, msg: 'denied' }, status));
    await expect(fetchSessions()).rejects.toMatchObject({ status, code, message: 'denied', requestId: 'req-ops' });
  });

  it('announces an expired legacy session on HTTP 401', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({ code: 1, msg: 'expired' }, 401));
    const listener = vi.fn();
    window.addEventListener('doris-ui:unauthorized', listener);

    await expect(fetchSessions()).rejects.toMatchObject({ status: 401, code: 'UI_UNAUTHENTICATED' });
    expect(listener).toHaveBeenCalledOnce();
    window.removeEventListener('doris-ui:unauthorized', listener);
  });

  it('announces an expired legacy session carried in an HTTP 200 envelope', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({ code: 401, msg: 'expired' }));
    const listener = vi.fn();
    window.addEventListener('doris-ui:unauthorized', listener);

    await expect(fetchSessions()).rejects.toMatchObject({
      status: 401,
      code: 'UI_UNAUTHENTICATED',
      message: 'expired',
    });
    expect(listener).toHaveBeenCalledOnce();
    window.removeEventListener('doris-ui:unauthorized', listener);
  });
});
