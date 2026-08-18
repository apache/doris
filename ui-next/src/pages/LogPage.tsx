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

import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Descriptions, Empty, Input, Popconfirm, Tag, message } from 'antd';
import { useState } from 'react';

import { UiApiError } from '../api/client';
import { addVerboseName, deleteVerboseName, logQueryKey, useLog } from '../api/log';
import { OperationState } from '../components/operations/OperationState';

function mutationError(error: unknown): string {
  if (!(error instanceof Error)) return 'The verbose logger could not be updated.';
  if (error instanceof UiApiError && error.requestId !== 'unknown') {
    return `${error.message} Request ID: ${error.requestId}`;
  }
  return error.message;
}

export function LogPage() {
  const queryClient = useQueryClient();
  const [messageApi, messageContext] = message.useMessage();
  const [newName, setNewName] = useState('');
  const query = useLog(true);
  const mutation = useMutation({
    mutationFn: ({ action, name }: { action: 'add' | 'delete'; name: string }) => (
      action === 'add' ? addVerboseName(name) : deleteVerboseName(name)
    ),
    onSuccess: (_result, variables) => {
      void queryClient.invalidateQueries({ queryKey: logQueryKey });
      if (variables.action === 'add') setNewName('');
      void messageApi.success(variables.action === 'add' ? 'Verbose logger added.' : 'Verbose logger deleted.');
    },
  });

  return (
    <main className="module-page operations-page log-page">
      {messageContext}
      <header className="page-heading">
        <h1>Log</h1>
      </header>
      <section className="operations-section" aria-labelledby="log-configuration-heading">
        <div className="section-heading">
          <div><p className="ui-label">Runtime settings</p><h2 id="log-configuration-heading">Log configuration</h2></div>
          <Button loading={query.isFetching} onClick={() => { void query.refetch(); }}>Refresh</Button>
        </div>
        <OperationState
          loading={query.isPending}
          error={query.error}
          hasData={Boolean(query.data)}
          partialFailures={query.data?.contentError ? [query.data.contentError] : []}
          onRetry={() => { void query.refetch(); }}
        />
        {query.data && (
          <>
            <Descriptions bordered column={{ xs: 1, sm: 2, lg: 4 }} size="small">
              <Descriptions.Item label="Level">{query.data.level || '—'}</Descriptions.Item>
              <Descriptions.Item label="Mode">{query.data.mode || '—'}</Descriptions.Item>
              <Descriptions.Item label="Audit names" span={2}>
                {query.data.auditNames.length > 0 ? query.data.auditNames.join(', ') : '—'}
              </Descriptions.Item>
            </Descriptions>
            <div className="verbose-section">
              <div className="section-heading compact-heading">
                <div><p className="ui-label">Debug scope</p><h3>Verbose names</h3></div>
              </div>
              <form
                  className="verbose-form"
                  onSubmit={(event) => {
                    event.preventDefault();
                    if (newName.trim()) mutation.mutate({ action: 'add', name: newName.trim() });
                  }}
                >
                  <Input
                    aria-label="New verbose logger name"
                    placeholder="org.apache.doris.example"
                    maxLength={256}
                    value={newName}
                    disabled={mutation.isPending}
                    onChange={(event) => setNewName(event.target.value)}
                  />
                  <Button type="primary" htmlType="submit" loading={mutation.isPending} disabled={!newName.trim()}>
                    Add verbose name
                  </Button>
                </form>
              {mutation.error && <Alert className="mutation-error" type="error" showIcon title={mutationError(mutation.error)} />}
              {query.data.verboseNames.length === 0 ? (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No verbose logger names configured." />
              ) : (
                <div className="verbose-list" aria-label="Verbose logger names">
                  {query.data.verboseNames.map((name) => (
                    <Tag key={name} className="verbose-tag">
                      <code>{name}</code>
                      <Popconfirm
                          title="Delete verbose logger?"
                          description={name}
                          okText="Delete"
                          okButtonProps={{ danger: true }}
                          onConfirm={() => mutation.mutate({ action: 'delete', name })}
                        >
                          <Button type="text" danger size="small" aria-label={`Delete ${name}`} disabled={mutation.isPending}>Delete</Button>
                        </Popconfirm>
                    </Tag>
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </section>
      {query.data && (
        <section className="operations-section log-contents-section" aria-labelledby="log-contents-heading">
          <div className="section-heading">
            <div><p className="ui-label">FE warning stream</p><h2 id="log-contents-heading">Log contents</h2></div>
            <span>{query.data.showingLastBytes.toLocaleString()} bytes shown</span>
          </div>
          <p className="log-path"><strong>Path</strong><code>{query.data.logPath || '—'}</code></p>
          <pre className="log-viewer" tabIndex={0}>{query.data.contents || 'No log content returned.'}</pre>
        </section>
      )}
    </main>
  );
}
