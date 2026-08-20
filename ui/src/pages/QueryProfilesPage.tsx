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

import { Button, Input, Space, Statistic, Tabs, message } from 'antd';
import { useMemo, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';

import { useQueryProfiles, useQueryProfileText } from '../api/queryProfiles';
import { DynamicDataTable } from '../components/operations/DynamicDataTable';
import { OperationState } from '../components/operations/OperationState';
import { ProfileGraph } from './queryProfiles/ProfileGraph';
import { copyProfileText, findProfileMatches } from './queryProfiles/profileTextSearch';
import { useProfileGraph } from './queryProfiles/useProfileGraph';

function profileRoute(profileId: string): string {
  return `/query-profiles/${encodeURIComponent(profileId)}`;
}

function downloadProfile(profileId: string, text: string) {
  const blobUrl = URL.createObjectURL(new Blob([text], { type: 'text/plain;charset=utf-8' }));
  const anchor = document.createElement('a');
  anchor.href = blobUrl;
  anchor.download = `profile_${profileId}.txt`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(blobUrl);
}

function QueryProfileList() {
  const query = useQueryProfiles();
  const profileColumn = query.data?.columnNames.indexOf('Profile ID') ?? -1;

  return (
    <main className="module-page operations-page query-profiles-page">
      <header className="page-heading">
        <h1>Query Profiles</h1>
      </header>
      <section className="operations-section" aria-labelledby="query-profiles-heading">
        <div className="section-heading">
          <div><p className="ui-label">Finished queries</p><h2 id="query-profiles-heading">Retained profiles</h2></div>
          <Statistic title="Profiles" value={query.data?.rows.length ?? 0} />
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
            searchPlaceholder="Filter query profiles"
            updatedAt={query.dataUpdatedAt}
            onRefresh={() => { void query.refetch(); }}
            renderCell={({ displayValue, columnIndex }) => (
              columnIndex === profileColumn && displayValue !== '—'
                ? <Link to={profileRoute(displayValue)}>{displayValue}</Link>
                : undefined
            )}
          />
        )}
      </section>
    </main>
  );
}

function QueryProfileDetail({ profileId }: { profileId: string }) {
  const query = useQueryProfileText(profileId);
  const [messageApi, messageContext] = message.useMessage();
  const [search, setSearch] = useState('');
  const [activeMatch, setActiveMatch] = useState(-1);
  const [activeTab, setActiveTab] = useState('text');
  const textArea = useRef<HTMLTextAreaElement>(null);
  const matches = useMemo(() => findProfileMatches(query.data ?? '', search), [query.data, search]);
  const graph = useProfileGraph(profileId, query.data, activeTab === 'visual');

  const focusMatch = (requestedIndex: number) => {
    if (matches.length === 0) return;
    const nextIndex = ((requestedIndex % matches.length) + matches.length) % matches.length;
    const offset = matches[nextIndex];
    setActiveMatch(nextIndex);
    requestAnimationFrame(() => {
      textArea.current?.focus();
      textArea.current?.setSelectionRange(offset, offset + search.trim().length);
    });
  };

  return (
    <main className="module-page operations-page query-profile-detail-page">
      {messageContext}
      <header className="page-heading">
        <h1>Query Profile</h1>
        <p className="profile-id" title={profileId}>{profileId}</p>
      </header>
      <section className="operations-section" aria-labelledby="profile-text-heading">
        <div className="section-heading profile-detail-heading">
          <div className="profile-detail-title">
            <Link className="profile-back-link" to="/query-profiles" aria-label="Back to query profiles">&lt;</Link>
            <div><p className="ui-label">Runtime profile</p><h2 id="profile-text-heading">Profile contents</h2></div>
          </div>
          <Space wrap>
            <Button
              disabled={!query.data}
              onClick={() => {
                if (!query.data) return;
                void copyProfileText(query.data)
                  .then(() => messageApi.success('Profile copied.'))
                  .catch(() => messageApi.error('The profile could not be copied.'));
              }}
            >Copy</Button>
            <Button
              disabled={!query.data}
              onClick={() => { if (query.data) downloadProfile(profileId, query.data); }}
            >Download</Button>
          </Space>
        </div>
        <OperationState
          loading={query.isPending}
          error={query.error}
          hasData={Boolean(query.data)}
          onRetry={() => { void query.refetch(); }}
        />
        {query.data !== undefined && (
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            items={[
              {
                key: 'text',
                label: 'Text',
                children: (
                  <>
                    <div className="profile-search-toolbar">
                      <Input.Search
                        allowClear
                        aria-label="Search profile text"
                        placeholder="Search profile text"
                        value={search}
                        onChange={(event) => {
                          setSearch(event.target.value);
                          setActiveMatch(-1);
                        }}
                        onSearch={() => focusMatch(activeMatch + 1)}
                      />
                      <Button disabled={matches.length === 0} onClick={() => focusMatch(activeMatch - 1)}>Previous</Button>
                      <Button disabled={matches.length === 0} onClick={() => focusMatch(activeMatch + 1)}>Next</Button>
                      <span aria-live="polite">
                        {search.trim() ? `${matches.length === 0 ? 0 : activeMatch + 1} of ${matches.length}` : 'Enter a search term'}
                      </span>
                    </div>
                    <textarea
                      ref={textArea}
                      className="profile-text-viewer"
                      aria-label="Query Profile text"
                      readOnly
                      spellCheck={false}
                      value={query.data}
                    />
                  </>
                ),
              },
              {
                key: 'visual',
                label: 'Visual',
                children: <ProfileGraph state={graph.state} graph={graph.graph} error={graph.error} />,
              },
            ]}
          />
        )}
      </section>
    </main>
  );
}

export function QueryProfilesPage() {
  const { profileId } = useParams<{ profileId: string }>();
  return profileId ? <QueryProfileDetail profileId={profileId} /> : <QueryProfileList />;
}
