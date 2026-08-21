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

import { adaptConfiguration, fetchConfiguration, updateConfiguration } from './configuration';
import { setCsrfToken } from './csrf';

function json(data: unknown) {
  return new Response(JSON.stringify({ code: 0, msg: 'success', data }), {
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('configuration adapter', () => {
  afterEach(() => {
    setCsrfToken(null);
    vi.restoreAllMocks();
  });

  it('normalizes FE rows and preserves per-node values and mutable state', () => {
    const rows = adaptConfiguration({
      column_names: ['配置项', '节点', '节点类型', '配置值类型', 'MasterOnly', '配置值', '可修改'],
      rows: [
        ['web_sql_max_result_bytes', 'fe-a:8030', 'FE', 'long', 'false', '10485760', 'false'],
        ['web_sql_max_result_bytes', 'fe-b:8030', 'FE', 'long', 'false', '20971520', 'true'],
      ],
    }, 'fe');

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ masterOnly: false, currentValue: '10485760', mutable: false });
    expect(rows[1]).toMatchObject({ node: 'fe-b:8030', currentValue: '20971520', mutable: true });
  });

  it('normalizes the BE schema without inventing Master Only', () => {
    const [row] = adaptConfiguration({
      column_names: ['配置项', '节点', '节点类型', '配置值类型', '配置值', '可修改'],
      rows: [['be_port', 'be-a:8040', 'BE', 'int32_t', '9060', 'true']],
    }, 'be');

    expect(row).toMatchObject({ name: 'be_port', masterOnly: null, currentValue: '9060', mutable: true });
  });

  it('uses the existing manager endpoint with cookie credentials and CSRF', async () => {
    setCsrfToken('csrf-config');
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({ column_names: [], rows: [] }));

    await fetchConfiguration('fe');

    expect(fetchSpy).toHaveBeenCalledWith(
      '/rest/v2/manager/node/configuration_info?type=fe',
      expect.objectContaining({ method: 'POST', credentials: 'same-origin', body: '{}' }),
    );
    const [, init] = fetchSpy.mock.calls[0];
    expect(new Headers(init?.headers).get('X-Doris-CSRF-Token')).toBe('csrf-config');
  });

  it('uses the existing set-config endpoint with CSRF and preserves per-node failures', async () => {
    setCsrfToken('csrf-config');
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      failed: [{ config_name: 'runtime_filter_type', value: '8 & 4', node: 'fe-b:8030', err_info: 'invalid value' }],
    }));

    await expect(updateConfiguration({
      scope: 'fe',
      name: 'runtime_filter_type',
      nodes: ['fe-a:8030', 'fe-b:8030'],
      value: '8 & 4',
      persist: true,
    })).resolves.toEqual({
      failures: [{
        configName: 'runtime_filter_type',
        value: '8 & 4',
        node: 'fe-b:8030',
        error: 'invalid value',
      }],
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(fetchSpy.mock.calls[0][0]).toBe('/rest/v2/manager/node/set_config/fe');
    expect(new Headers(init?.headers).get('X-Doris-CSRF-Token')).toBe('csrf-config');
    if (typeof init?.body !== 'string') throw new Error('Expected a JSON request body.');
    expect(JSON.parse(init.body)).toEqual({
      runtime_filter_type: {
        node: ['fe-a:8030', 'fe-b:8030'],
        value: '8 & 4',
        persist: true,
      },
    });
  });
});
