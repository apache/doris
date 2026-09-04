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

import { fetchQueryProfiles, fetchQueryProfileText } from './queryProfiles';

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('Query Profile API adapter', () => {
  it('reuses the current-FE legacy list and preserves the Profile ID link', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 0,
      msg: 'success',
      data: {
        column_names: ['Profile ID', 'Task Type', 'Sql Statement'],
        href_column: ['Profile ID'],
        rows: [
          {
            'Profile ID': 'aaaa-bbbb',
            'Task Type': 'QUERY',
            'Sql Statement': 'select 1',
            __hrefPaths: ['/query_profile/aaaa-bbbb'],
          },
          {
            'Profile ID': 'load-01',
            'Task Type': 'LOAD',
            'Sql Statement': 'insert into table values (1)',
            __hrefPaths: ['/query_profile/load-01'],
          },
        ],
      },
    }));

    await expect(fetchQueryProfiles()).resolves.toEqual({
      columnNames: ['Profile ID', 'Task Type', 'Sql Statement'],
      rows: [{ key: 'row-0', cells: ['aaaa-bbbb', 'QUERY', 'select 1'], links: { 0: '/query_profile/aaaa-bbbb' } }],
    });
    expect(fetchSpy).toHaveBeenCalledWith('/rest/v1/query_profile', expect.objectContaining({ method: 'GET' }));
  });

  it('URL-encodes the Profile ID and returns text without transforming it', async () => {
    const profile = 'Summary:\n  - Doris Version: doris-4.1.0\nMergedProfile:\n';
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({ code: 0, msg: 'success', data: profile }));

    await expect(fetchQueryProfileText('id/with space')).resolves.toBe(profile);
    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v1/query_profile/text/id%2Fwith%20space',
      expect.objectContaining({ method: 'GET', credentials: 'same-origin' }),
    );
  });

  it('rejects an empty Profile ID before sending a request', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch');
    await expect(fetchQueryProfileText('  ')).rejects.toThrow('A Profile ID is required.');
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
