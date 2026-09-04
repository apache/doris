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

import { Alert, Button, Skeleton } from 'antd';

import { UiApiError } from '../../api/client';

interface OperationStateProps {
  loading: boolean;
  error?: unknown;
  permissionDenied?: boolean;
  partialFailures?: string[];
  hasData?: boolean;
  onRetry?: () => void;
}

export function OperationState({
  loading,
  error,
  permissionDenied = false,
  partialFailures = [],
  hasData = false,
  onRetry,
}: OperationStateProps) {
  if (permissionDenied) {
    return <Alert type="warning" showIcon title="Permission required" description="This account cannot view this operational page." />;
  }
  if (error && !hasData) {
    const requestId = error instanceof UiApiError ? error.requestId : null;
    return (
      <Alert
        type="error"
        showIcon
        title={error instanceof Error ? error.message : 'The operational data could not be loaded.'}
        description={requestId && requestId !== 'unknown' ? `Request ID: ${requestId}` : undefined}
        action={onRetry ? <Button onClick={onRetry}>Retry</Button> : undefined}
      />
    );
  }
  if (loading && !hasData) return <Skeleton active paragraph={{ rows: 6 }} />;
  if (partialFailures.length > 0) {
    return (
      <Alert
        type="warning"
        showIcon
        title="Some data could not be loaded"
        description={partialFailures.join(' ')}
        action={onRetry ? <Button onClick={onRetry}>Retry</Button> : undefined}
      />
    );
  }
  return null;
}
