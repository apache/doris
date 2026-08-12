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

import { fetchBackends, fetchFrontends, fetchVersion } from './home';

function json(data: unknown) {
  return new Response(JSON.stringify({ data, requestId: 'req-home' }), {
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-home' },
  });
}

describe('Home API', () => {
  it('uses the dedicated Version and node status facades', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(json({ version: '4.0', git: 'abc', buildInfo: 'builder', buildTime: 'today', features: 'x' }))
      .mockResolvedValueOnce(json({ columnNames: ['Name', 'FutureField'], rows: [['fe-1', 'kept']] }))
      .mockResolvedValueOnce(json({ columnNames: ['BackendId'], rows: [['10001']] }));

    await expect(fetchVersion()).resolves.toMatchObject({ version: '4.0' });
    await expect(fetchFrontends()).resolves.toEqual({ columnNames: ['Name', 'FutureField'], rows: [['fe-1', 'kept']] });
    await expect(fetchBackends()).resolves.toEqual({ columnNames: ['BackendId'], rows: [['10001']] });

    expect(fetchSpy.mock.calls.map(([path]) => path)).toEqual([
      '/rest/v1/ui/home/version',
      '/rest/v1/ui/nodes/frontends',
      '/rest/v1/ui/nodes/backends',
    ]);
  });
});
