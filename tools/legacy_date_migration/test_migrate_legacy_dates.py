# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as parquet

from migrate_legacy_dates import convert_array, convert_table, migrate


class LegacyDateMigrationTest(unittest.TestCase):
    def test_boundary_ordinals_and_nulls(self):
        raw = [-719527, -719526, -719469, -719468, -719162, -1, 0, 19723, 2932896, None]
        expected = [-719528, -719527, -719470] + raw[3:]
        result = convert_array(pa.array(raw, pa.date32()))
        self.assertEqual(expected, result.cast(pa.int32()).to_pylist())

    def test_nested_values_and_slices(self):
        date = pa.date32()
        types_and_values = [
            (pa.list_(date), [[-719527], None, [-719469, None], []]),
            (pa.large_list(date), [[-719527], None, [-719469, None], []]),
            (pa.list_(date, 2), [[-719527, 0], None, [-719469, None], [0, 1]]),
            (pa.struct([('d', date), ('s', pa.string())]),
             [{'d': -719527, 's': 'a'}, None, {'d': -719469, 's': 'b'}, {'d': None}]),
            (pa.map_(pa.string(), pa.list_(date)),
             [[('a', [-719527])], None, [('b', [-719469, None])], []]),
            (pa.map_(date, pa.string()), [[(-719527, 'a')], None, [(-719469, 'b')], []]),
        ]
        for data_type, values in types_and_values:
            with self.subTest(data_type=data_type):
                original = pa.array(values, data_type)
                converted = convert_array(original)
                self.assertEqual(original.type, converted.type)
                self.assertEqual(original.is_null(), converted.is_null())
                self.assertEqual(converted.slice(1, 2), convert_array(original.slice(1, 2)))
        array = convert_array(pa.array([[-719527, -719469, None]], pa.list_(date)))
        self.assertEqual([-719528, -719470, None], array.values.cast(pa.int32()).to_pylist())

    def test_null_parent_hides_invalid_date_payload(self):
        hidden = pa.array([2147483647, -719527], pa.date32())
        mask = pa.array([True, False])
        arrays = [
            pa.StructArray.from_arrays([hidden], names=['d'], mask=mask),
            pa.ListArray.from_arrays([0, 1, 2], hidden, mask=mask),
            pa.FixedSizeListArray.from_arrays(hidden, 1, mask=mask),
            pa.MapArray.from_arrays([0, 1, 2], pa.array(['a', 'b']), hidden, mask=mask),
            pa.MapArray.from_arrays([0, 1, 2], hidden, pa.array(['a', 'b']), mask=mask),
        ]
        for array in arrays:
            with self.subTest(data_type=array.type):
                result = convert_array(array)
                self.assertTrue(result[0].as_py() is None)
                # The visible second row is identical to converting it independently.
                self.assertEqual(result.slice(1), convert_array(array.slice(1)))

    def test_unused_dictionary_values_are_not_validated(self):
        dictionary = pa.array([-719527] + [2147483647] * 199, pa.date32())
        array = pa.DictionaryArray.from_arrays(pa.array([0, None], pa.int8()), dictionary)
        result = convert_array(array)
        self.assertEqual(array.indices, result.indices)
        self.assertEqual([-719528, None], result.dictionary_decode().cast(pa.int32()).to_pylist())
        invalid = pa.DictionaryArray.from_arrays(pa.array([1], pa.int8()), dictionary)
        with self.assertRaises(ValueError):
            convert_array(invalid)

    def test_metadata_chunks_and_other_types(self):
        schema = pa.schema([pa.field('d', pa.date32(), metadata={b'field': b'value'}),
                            pa.field('i', pa.int64(), nullable=False)], metadata={b'table': b'value'})
        table = pa.Table.from_arrays([
            pa.chunked_array([pa.array([-719527], pa.date32()), pa.array([None], pa.date32())]),
            pa.array([1, 2], pa.int64())], schema=schema)
        result = convert_table(table)
        self.assertTrue(result.schema.equals(schema, check_metadata=True))
        self.assertEqual(table['i'], result['i'])
        self.assertEqual(2, result['d'].num_chunks)
        self.assertEqual([-719528, None], result['d'].cast(pa.int32()).to_pylist())

    def test_round_trip_files_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            for fmt in ['orc', 'parquet']:
                source = Path(directory) / ('source.' + fmt)
                output = Path(directory) / ('output.' + fmt)
                table = pa.table({'d': pa.array([-719527, -719469, -719468, 0, 2932896, None],
                                                pa.date32())})
                module = orc if fmt == 'orc' else parquet
                module.write_table(table, source)
                before = source.read_bytes()
                with self.assertRaises(ValueError):
                    migrate(source, output, fmt)
                migrate(source, output, fmt, True)
                self.assertEqual([-719528, -719470, -719468, 0, 2932896, None],
                                 module.read_table(output)['d'].cast(pa.int32()).to_pylist())
                with self.assertRaises(FileExistsError):
                    migrate(source, output, fmt, True)
                with self.assertRaises(ValueError):
                    migrate(source, source, fmt, True)
                # A corrected lower-bound value is outside the legacy encoding's domain.
                with self.assertRaises(ValueError):
                    migrate(output, Path(directory) / ('twice.' + fmt), fmt, True)
                self.assertEqual(before, source.read_bytes())
                self.assertFalse(list(Path(directory).glob('.legacy-date-*')))

    def test_failure_never_publishes_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / 'source.parquet', Path(directory) / 'output.parquet'
            parquet.write_table(pa.table({'d': pa.array([-719528], pa.date32())}), source)
            with self.assertRaises(ValueError):
                migrate(source, output, 'parquet', True)
            self.assertFalse(output.exists())
            self.assertFalse(list(Path(directory).glob('.legacy-date-*')))

    def test_concurrent_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / 'source.parquet', Path(directory) / 'output.parquet'
            parquet.write_table(pa.table({'d': pa.array([0], pa.date32())}), source)
            def racing_writer(*args):
                output.write_bytes(b'other writer')
                raise FileExistsError(output)
            with patch('migrate_legacy_dates.os.link', side_effect=racing_writer):
                with self.assertRaises(FileExistsError):
                    migrate(source, output, 'parquet', True)
            self.assertEqual(b'other writer', output.read_bytes())
            self.assertFalse(list(Path(directory).glob('.legacy-date-*')))

    def test_nested_file_values_and_timestamp_precision(self):
        schema = pa.schema([
            pa.field('nested', pa.struct([
                pa.field('dates', pa.list_(pa.date32()), metadata={b'list': b'metadata'}),
                pa.field('mapping', pa.map_(pa.string(), pa.date32()))])),
            pa.field('ts', pa.timestamp('us'), nullable=False)], metadata={b'table': b'metadata'})
        table = pa.Table.from_arrays([
            pa.array([{'dates': [-719527, None], 'mapping': [('a', -719469)]}, None],
                     schema.field('nested').type),
            pa.array([123456789, 987654321], pa.timestamp('us'))], schema=schema)
        expected = convert_table(table)
        with tempfile.TemporaryDirectory() as directory:
            for fmt, module in [('orc', orc), ('parquet', parquet)]:
                source, output = Path(directory) / ('s.' + fmt), Path(directory) / ('o.' + fmt)
                module.write_table(table, source)
                migrate(source, output, fmt, True)
                actual = module.read_table(output)
                self.assertEqual(expected['nested'].combine_chunks(),
                                 actual['nested'].combine_chunks().cast(schema.field('nested').type))
                self.assertEqual(expected['ts'], actual['ts'].cast(pa.timestamp('us')))
                if fmt == 'parquet':
                    self.assertTrue(actual.schema.equals(module.read_table(source).schema,
                                                         check_metadata=True))

    def test_empty_files(self):
        with tempfile.TemporaryDirectory() as directory:
            for fmt, module in [('orc', orc), ('parquet', parquet)]:
                source, output = Path(directory) / ('s.' + fmt), Path(directory) / ('o.' + fmt)
                module.write_table(pa.table({'d': pa.array([], pa.date32())}), source)
                migrate(source, output, fmt, True)
                self.assertEqual(pa.date32(), module.read_table(output).schema.field('d').type)


if __name__ == '__main__':
    unittest.main()
