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

import { fireEvent, render, screen } from '@testing-library/react';

import { UiApiError } from '../../api/client';
import { OperationState } from './OperationState';

describe('OperationState', () => {
  it('shows a defensive permission state', () => {
    render(<OperationState loading={false} permissionDenied />);
    expect(screen.getByText('Permission required')).toBeInTheDocument();
  });

  it('shows a request ID and retries a full failure', () => {
    const retry = vi.fn();
    render(<OperationState loading={false} error={new UiApiError(500, {
      code: 'UI_SERVER_ERROR', message: 'Failed', requestId: 'req-state',
    })} onRetry={retry} />);
    expect(screen.getByText('Request ID: req-state')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(retry).toHaveBeenCalledOnce();
  });

  it('keeps partial data visible while reporting partial failures', () => {
    render(<OperationState loading={false} hasData partialFailures={['Backends are unavailable.']} />);
    expect(screen.getByText('Some data could not be loaded')).toBeInTheDocument();
    expect(screen.getByText('Backends are unavailable.')).toBeInTheDocument();
  });
});
