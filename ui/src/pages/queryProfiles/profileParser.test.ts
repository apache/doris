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
import tpcdsQuery41MergedProfile from '../../test/fixtures/profiles/tpcds-query41-merged-profile.txt?raw';

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

  it('maps current Doris operator counters to their actual semantics', () => {
    // Captured MergedProfile shape and counter names emitted by Doris BE.
    const graph = parseProfileText([
      'MergedProfile:',
      '  Fragment 0:',
      '    Pipeline 0(instance_num=1):',
      '      AGGREGATION_SINK_OPERATOR(nereids_id=842)(id=3):',
      '        CommonCounters:',
      '          - InputRows: sum 6, avg 3, max 4, min 2',
      '          - RowsProduced: sum 2, avg 1, max 1, min 1',
      '          - MemoryUsage: sum 1.25 MB, avg 640.00 KB, max 1.00 MB, min 256.00 KB',
      '          - PeakMemoryUsage: sum 2.00 MB, avg 1.00 MB, max 1.50 MB, min 512.00 KB',
      'DetailProfile(test):',
    ].join('\n'));

    expect(graph.graph.nodes[0].metrics).toMatchObject({
      inputRows: { sum: 6, avg: 3, max: 4, min: 2 },
      outputRows: { sum: 2, avg: 1, max: 1, min: 1 },
      memoryUsageBytes: { sum: 1_310_720, avg: 655_360, max: 1_048_576, min: 262_144 },
      memoryPeakBytes: { sum: 2_097_152, avg: 1_048_576, max: 1_572_864, min: 524_288 },
    });
  });

  it('keeps the Doris 4.0 MemoryUsagePeak spelling as a compatibility fallback', () => {
    const profile = representativeProfile().replace(
      '          - ExecTime: avg 2ms, max 3ms, min 1ms',
      '          - MemoryUsagePeak: sum 2.00 MB, avg 2.00 MB, max 2.00 MB, min 2.00 MB',
    );
    expect(parseProfileText(profile).graph.nodes[0].metrics?.memoryPeakBytes?.max).toBe(2_097_152);
  });

  it('parses row and memory metrics from a captured Doris TPC-DS Profile', () => {
    const graph = parseProfileText(tpcdsQuery41MergedProfile);
    const aggregationSink = graph.graph.nodes.find(
      node => node.operatorType === 'AGGREGATION_SINK_OPERATOR' && node.nereidsId === 842,
    );

    expect(graph.summary).toMatchObject({ fragmentCount: 5, pipelineCount: 13 });
    expect(aggregationSink?.metrics).toMatchObject({
      inputRows: { sum: 6, max: 3 },
      outputRows: null,
      memoryPeakBytes: { sum: 1_310_720, max: 436_511 },
    });
  });

  it('pairs the BE partitioned hash join operator names', () => {
    const graph = parseProfileText([
      'MergedProfile:',
      '  Fragment 0:',
      '    Pipeline 0(instance_num=1):',
      '      PARTITIONED_HASH_JOIN_PROBE_OPERATOR(nereids_id=12)(id=4):',
      '    Pipeline 1(instance_num=1):',
      '      PARTITIONED_HASH_JOIN_SINK_OPERATOR(nereids_id=12)(id=4):',
      'DetailProfile(test):',
    ].join('\n'));

    expect(graph.graph.edges).toEqual([
      expect.objectContaining({ kind: 'BUILD_DEPENDENCY', relationId: '12' }),
    ]);
    expect(graph.unresolvedReferences).toHaveLength(0);
  });

  it('parses multicast dest_ids lists and reports unresolved branches', () => {
    const graph = parseProfileText([
      'MergedProfile:',
      '  Fragment 0:',
      '    Pipeline 0(instance_num=1):',
      '      MULTI_CAST_DATA_STREAM_SINK_OPERATOR(id=9, dest_ids=[7,8]):',
      '  Fragment 1:',
      '    Pipeline 0(instance_num=1):',
      '      MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR(id=7):',
      '  Fragment 2:',
      '    Pipeline 0(instance_num=1):',
      '      MULTI_CAST_DATA_STREAM_SOURCE_OPERATOR(id=8):',
      'DetailProfile(test):',
    ].join('\n'));

    expect(graph.graph.nodes[0].destIds).toEqual([7, 8]);
    expect(graph.graph.edges.filter(edge => edge.kind === 'MULTICAST')).toHaveLength(2);
    expect(graph.unresolvedReferences).toHaveLength(0);
  });
});

describe('MergedProfile parser diagnostic inputs', () => {
  const scanNode = () =>
    parseProfileText(tpcdsQuery41MergedProfile).graph.nodes.find(
      node => node.operatorType === 'OLAP_SCAN_OPERATOR' && node.fragmentId === 'fragment:2',
    )!;

  it('keeps the closing parenthesis of a parenthesised table name', () => {
    // table_name=item(item) lost its ")" while the character class still allowed "(".
    expect(scanNode().planInfo.table).toBe('item(item)');
  });

  it('exposes every counter, not only the four rendered metrics', () => {
    const counters = scanNode().counters!;
    expect(Object.keys(counters).length).toBeGreaterThan(4);
    expect(counters.ScanRows.sum).toBe(18_000);
    expect(counters.ScanBytes.max).toBe(146_289);
  });

  it('reads counters nested under a parent counter', () => {
    const counters = scanNode().counters!;
    expect(counters['RuntimeFilterInfo/RF0 FilterRows'].sum).toBe(10_491);
    expect(counters['RuntimeFilterInfo/RF0 InputRows'].sum).toBe(34_439);
    expect(counters['RuntimeFilterInfo/RF1 FilterRows'].sum).toBe(127);
  });

  it('groups nested runtime filter counters per filter', () => {
    expect(scanNode().runtimeFilters).toEqual([
      { id: 'RF0', alwaysTrueFilterRows: 0, filterRows: 10_491, inputRows: 34_439 },
      { id: 'RF1', alwaysTrueFilterRows: 0, filterRows: 127, inputRows: 127 },
    ]);
  });

  it('recovers numeric plan facts from PlanInfo free text', () => {
    expect(scanNode().planFacts).toEqual({
      cardinality: 18_000,
      avgRowSize: 449.79193,
      numNodes: 1,
      partitionsSelected: 1,
      partitionsTotal: 1,
      tabletsSelected: 10,
      tabletsTotal: 10,
      pushAggOp: 'NONE',
      preAggregation: 'ON',
      predicates: '((i_manufact_id >= 748) AND (i_manufact_id <= 788))',
    });
  });

  it('splits a PlanInfo line that carries two pairs', () => {
    const graph = parseProfileText([
      'MergedProfile:',
      '  Fragment 0:',
      '    Pipeline 0(instance_num=1):',
      '      OLAP_SCAN_OPERATOR(id=0):',
      '        - PlanInfo',
      '           - TABLE: tpcds.store_sales(store_sales), PREAGGREGATION: OFF',
      '           - partitions=3/120 (p1,p2,p3)',
      '           - tablets=6/240, tabletList=1,2,3',
      'DetailProfile(test):',
    ].join('\n'));

    const node = graph.graph.nodes[0];
    expect(node.planInfo.table).toBe('tpcds.store_sales(store_sales)');
    expect(node.planFacts).toMatchObject({
      preAggregation: 'OFF',
      partitionsSelected: 3,
      partitionsTotal: 120,
      tabletsSelected: 6,
      tabletsTotal: 240,
    });
  });

  it('omits planFacts and runtimeFilters when the operator reports neither', () => {
    const node = parseProfileText(representativeProfile()).graph.nodes[0];
    expect(node.planFacts).toBeUndefined();
    expect(node.runtimeFilters).toBeUndefined();
  });
});
