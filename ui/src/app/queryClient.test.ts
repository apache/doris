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

import { UiApiError } from '../api/client';
import { retryUiQuery } from './queryClient';

function apiError(status: number) {
  return new UiApiError(status, {
    code: 'TEST',
    message: 'test',
    requestId: 'req-test',
  });
}

describe('retryUiQuery', () => {
  it('retries a GET server failure only once', () => {
    expect(retryUiQuery(0, apiError(500))).toBe(true);
    expect(retryUiQuery(1, apiError(500))).toBe(false);
  });

  it('does not retry authentication, permission, or client failures', () => {
    expect(retryUiQuery(0, apiError(401))).toBe(false);
    expect(retryUiQuery(0, apiError(403))).toBe(false);
    expect(retryUiQuery(0, apiError(429))).toBe(false);
    expect(retryUiQuery(0, new Error('network'))).toBe(false);
  });
});
