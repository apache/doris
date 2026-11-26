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

import { Alert, Spin } from 'antd';
import { Navigate, useLocation } from 'react-router-dom';

import { UiApiError } from '../api/client';
import { useMe } from '../api/me';
import { AppShell } from './AppShell';

export function AuthGate() {
  const location = useLocation();
  const me = useMe();

  if (me.isPending) {
    return (
      <main className="full-page-state" aria-label="Checking session">
        <Spin size="large" />
        <p>Checking your Doris session…</p>
      </main>
    );
  }

  if (me.error instanceof UiApiError && me.error.status === 401) {
    const from = `${location.pathname}${location.search}${location.hash}`;
    return <Navigate to="/login" replace state={{ reason: 'expired', from }} />;
  }

  if (me.isError) {
    return (
      <main className="full-page-state">
        <Alert
          type="error"
          showIcon
          title="The Doris FE is unavailable"
          description="The application could not verify your session. Refresh the page when the FE is available."
        />
      </main>
    );
  }

  return <AppShell me={me.data} />;
}
