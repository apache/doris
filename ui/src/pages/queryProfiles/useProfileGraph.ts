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

import { useEffect, useState } from 'react';

import type { DagUiState, ProfileGraphIR } from './profileGraphTypes';
import { ProfileParserError } from './profileParser';
import { startProfileParse, type ProfileParseOperation } from './profileParserClient';

export interface ProfileGraphSnapshot {
  state: DagUiState;
  graph: ProfileGraphIR | null;
  error: string | null;
}

const idle: ProfileGraphSnapshot = { state: 'idle', graph: null, error: null };

function createWorker() {
  return new Worker(new URL('./profileParser.worker.ts', import.meta.url), { type: 'module' });
}

export function useProfileGraph(profileId: string, text: string | undefined, enabled: boolean): ProfileGraphSnapshot {
  const [snapshot, setSnapshot] = useState<ProfileGraphSnapshot>(idle);

  useEffect(() => {
    if (!enabled || text === undefined) {
      return undefined;
    }

    let active = true;
    const operation: ProfileParseOperation = startProfileParse(text, createWorker);
    queueMicrotask(() => { if (active) setSnapshot({ state: 'parsing', graph: null, error: null }); });
    void operation.promise.then(
      (graph) => { if (active) setSnapshot({ state: 'ready', graph, error: null }); },
      (reason: unknown) => {
        if (!active || (reason instanceof Error && reason.name === 'AbortError')) return;
        const code = reason instanceof ProfileParserError ? reason.code : 'DAG_PARSE_FAILED';
        setSnapshot({
          state: code === 'DAG_PARSE_FAILED' ? 'failed' : 'unavailable',
          graph: null,
          error: reason instanceof Error ? reason.message : 'The execution graph could not be generated.',
        });
      },
    );
    return () => {
      active = false;
      operation.cancel();
    };
  }, [enabled, profileId, text]);

  return enabled && text !== undefined ? snapshot : idle;
}
