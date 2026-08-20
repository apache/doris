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

import {
  parseCounter,
  parseProfileText,
  ProfileParserError,
} from './profileParser';

function representativeProfile(version = '4.1.0') {
  return [
    'Summary:',
    `  - Doris Version: doris-${version}-abcdef`,
    'MergedProfile:',
    '  Fragment 0:',
    '    Pipeline 0(instance_num=1):',
    '      RESULT_SINK_OPERATOR(id=2147483647):',
    '        CommonCounters:',
    '          - ExecTime: avg 2ms, max 3ms, min 1ms',
    '      EXCHANGE_OPERATOR(id=35):',
    '        CommonCounters:',
    '          - ExecTime: avg 1ms, max 1ms, min 1ms',
    '  Fragment 1:',
    '    Pipeline 0(instance_num=2):',
    '      DATA_STREAM_SINK_OPERATOR(dest_id=35):',
    '        CommonCounters:',
    '          - ExecTime: avg 1ms, max 2ms, min 1ms',
    'DetailProfile(test):',
  ].join('\n');
}

describe('MergedProfile parser', () => {
  it('parses a graph with pipeline and exchange edges', () => {
    const graph = parseProfileText(representativeProfile());
    expect(graph.summary).toMatchObject({ fragmentCount: 2, pipelineCount: 2, nodeCount: 3, edgeCount: 2 });
    expect(graph.graph.edges.map((edge) => edge.kind)).toEqual(['PIPELINE_DATA', 'EXCHANGE']);
    expect(graph.summary.criticalNodeId).toBe('fragment:0/pipeline:0/operator:0');
  });

  it.each(['4.0.6', '4.1.0', '5.0.0'])('parses a structurally compatible Doris %s Profile', (version) => {
    expect(parseProfileText(representativeProfile(version)).summary.nodeCount).toBe(3);
  });

  it('does not require a Doris version marker when MergedProfile is compatible', () => {
    const profile = representativeProfile().replace(/^ {2}- Doris Version:.*\n/m, '');
    expect(parseProfileText(profile).summary.nodeCount).toBe(3);
  });

  it('reports Visual as unavailable when MergedProfile is absent', () => {
    expect(() => parseProfileText('Summary:\n  - Task Type: QUERY')).toThrowError(
      expect.objectContaining({ code: 'DAG_UNAVAILABLE' }),
    );
  });

  it('reports an incompatible MergedProfile structure as a parse failure with a reason', () => {
    try {
      parseProfileText('MergedProfile:\n  incompatible content');
      throw new Error('Expected parsing to fail.');
    } catch (error) {
      expect(error).toBeInstanceOf(ProfileParserError);
      if (!(error instanceof ProfileParserError)) return;
      expect(error.code).toBe('DAG_PARSE_FAILED');
      expect(error.message).toContain('Fragment, Pipeline, and Operator');
    }
  });

  it('keeps compound duration and exact count parsing from the website baseline', () => {
    expect(parseCounter('WaitForDependencyTime', 'avg 13sec796ms, max 13sec796ms, min 1us')).toEqual({
      sum: undefined,
      avg: 13_796_000_000,
      max: 13_796_000_000,
      min: 1_000,
    });
    expect(parseCounter('RowsProduced', 'sum 2.232K (2232), avg 279, max 309, min 253')).toEqual({
      sum: 2232,
      avg: 279,
      max: 309,
      min: 253,
    });
  });
});
