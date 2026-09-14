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

import { Statistic } from 'antd';

import { useSessions } from '../api/operations';
import { DynamicDataTable } from '../components/operations/DynamicDataTable';
import { OperationState } from '../components/operations/OperationState';

export function SessionsPage() {
  const query = useSessions(true);

  return (
    <main className="module-page operations-page">
      <header className="page-heading">
        <h1>Sessions</h1>
      </header>
      <section className="operations-section" aria-labelledby="sessions-heading">
        <div className="section-heading">
          <div><p className="ui-label">Live process list</p><h2 id="sessions-heading">Current sessions</h2></div>
          <Statistic title="Active sessions" value={query.data?.rows.length ?? 0} />
        </div>
        <OperationState
          loading={query.isPending}
          error={query.error}
          hasData={Boolean(query.data)}
          onRetry={() => { void query.refetch(); }}
        />
        {query.data && (
          <DynamicDataTable
            data={query.data}
            loading={query.isFetching}
            searchPlaceholder="Filter sessions"
            updatedAt={query.dataUpdatedAt}
            onRefresh={() => { void query.refetch(); }}
          />
        )}
      </section>
    </main>
  );
}
