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

import { Breadcrumb, Button } from 'antd';
import { Link, useSearchParams } from 'react-router-dom';

import { normalizeProcPath, procPathFromHref, useSystem } from '../api/operations';
import { DynamicDataTable } from '../components/operations/DynamicDataTable';
import { OperationState } from '../components/operations/OperationState';
import { parentProcPath, procBreadcrumbs, systemRoute } from './systemPath';

export function SystemPage() {
  const [searchParams] = useSearchParams();
  const path = normalizeProcPath(searchParams.get('path') ?? '/');
  const query = useSystem(path, true);
  const fallbackParent = parentProcPath(path);
  const parent = path === '/' ? null : (query.data?.parentPath ?? fallbackParent);

  return (
    <main className="module-page operations-page">
      <header className="page-heading">
        <h1>Proc System</h1>
      </header>
      <section className="operations-section" aria-labelledby="system-data-heading">
        <div className="section-heading">
          <div><p className="ui-label">Current Proc path</p><h2 id="system-data-heading">{path}</h2></div>
          {parent && <Link to={systemRoute(parent)}><Button>Parent directory</Button></Link>}
        </div>
        <Breadcrumb
          className="proc-breadcrumb"
          items={procBreadcrumbs(path).map((item, index, items) => ({
            title: index === items.length - 1 ? item.label : <Link to={systemRoute(item.path)}>{item.label}</Link>,
          }))}
        />
        <OperationState
          loading={query.isPending}
          error={query.error}
          hasData={Boolean(query.data)}
          onRetry={() => { void query.refetch(); }}
        />
        {query.data && (
          <DynamicDataTable
            data={query.data.table}
            loading={query.isFetching}
            searchPlaceholder="Filter this Proc directory"
            updatedAt={query.dataUpdatedAt}
            onRefresh={() => { void query.refetch(); }}
            renderCell={({ displayValue, row, columnIndex }) => {
              const href = row.links?.[columnIndex];
              if (!href) return undefined;
              const childPath = procPathFromHref(href);
              if (childPath) return <Link to={systemRoute(childPath)}>{displayValue}</Link>;
              return <a href={href} target="_blank" rel="noreferrer">{displayValue}</a>;
            }}
          />
        )}
      </section>
    </main>
  );
}
