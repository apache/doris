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

import { DynamicDataTable } from './DynamicDataTable';

describe('DynamicDataTable', () => {
  it('uses stable unique column keys for unknown and duplicate column names', () => {
    render(
      <DynamicDataTable
        data={{
          columnNames: ['Name', 'Name', 'Previously unseen column'],
          rows: [{ key: '1', cells: ['first', 'second', 'a very long value that remains inspectable'] }],
        }}
      />,
    );
    expect(screen.getAllByRole('columnheader', { name: 'Name' })).toHaveLength(2);
    expect(screen.getByRole('columnheader', { name: 'Previously unseen column' })).toBeInTheDocument();
    expect(screen.getAllByTitle(/a very long value/).length).toBeGreaterThan(0);
  });

  it('filters all cells and provides a useful empty result', () => {
    render(
      <DynamicDataTable
        data={{
          columnNames: ['Id', 'State'],
          rows: [{ key: '1', cells: ['9007199254740993', 'Sleep'] }],
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText('Filter table'), { target: { value: 'missing' } });
    expect(screen.getByText('No rows match this filter.')).toBeInTheDocument();
  });
});
