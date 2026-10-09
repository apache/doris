#!/usr/bin/env python3
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

"""Rewrite confirmed legacy Doris DATE32 files into proleptic Gregorian encoding."""

import argparse
import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.orc as orc
import pyarrow.parquet as parquet


def convert_array(array, visible=None):
    """Preserve Arrow structure and nulls while correcting legacy DATE32 ordinals."""
    data_type = array.type
    visible = array.is_valid() if visible is None else pc.and_(visible, array.is_valid())
    if pa.types.is_date32(data_type):
        # Children beneath a null container have no date value, regardless of their payload.
        days = pc.if_else(visible, array.cast(pa.int32()), pa.scalar(None, pa.int32()))
        bounds = pc.min_max(days).as_py()
        if bounds['min'] is not None and (
                bounds['min'] < -719527 or bounds['max'] > 2932896):
            raise ValueError('DATE value outside the legacy Doris range')
        # The old MySQL-calendar encoder is one day ahead only before March of year zero.
        early = pc.and_(pc.greater_equal(days, -719527), pc.less_equal(days, -719469))
        return pc.if_else(early, pc.subtract(days, pa.scalar(1, pa.int32())), days).cast(pa.date32())
    if pa.types.is_struct(data_type):
        return pa.StructArray.from_arrays(
            [convert_array(array.field(i), visible) for i in range(data_type.num_fields)],
            fields=list(data_type), mask=array.is_null())
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        start, end = array.offsets[0].as_py(), array.offsets[-1].as_py()
        raw_values = array.values.slice(start, end - start)
        offsets = pc.subtract(array.offsets, start)
        parents = pc.list_parent_indices(pa.LargeListArray.from_arrays(offsets, raw_values))
        values = convert_array(raw_values, pc.take(visible, parents))
        constructor = pa.LargeListArray if pa.types.is_large_list(data_type) else pa.ListArray
        return constructor.from_arrays(offsets, values, type=data_type, mask=array.is_null())
    if pa.types.is_fixed_size_list(data_type):
        size = data_type.list_size
        raw_values = array.values.slice(array.offset * size, len(array) * size)
        parents = pc.list_parent_indices(pa.FixedSizeListArray.from_arrays(raw_values, size))
        values = convert_array(raw_values, pc.take(visible, parents))
        return pa.FixedSizeListArray.from_arrays(values, type=data_type, mask=array.is_null())
    if pa.types.is_map(data_type):
        start, end = array.offsets[0].as_py(), array.offsets[-1].as_py()
        offsets = pc.subtract(array.offsets, start)
        keys = array.keys.slice(start, end - start)
        parents = pc.list_parent_indices(pa.LargeListArray.from_arrays(offsets, keys))
        child_visible = pc.take(visible, parents)
        converted_keys = convert_array(keys, child_visible)
        if pa.types.is_date32(keys.type):
            # Arrow requires non-null map keys even in an unreachable null map slot.
            converted_keys = pc.fill_null(converted_keys, pa.scalar(0, pa.date32()))
        return pa.MapArray.from_arrays(
            offsets,
            converted_keys,
            convert_array(array.items.slice(start, end - start), child_visible),
            type=data_type, mask=array.is_null())
    if pa.types.is_dictionary(data_type):
        used = pc.unique(pc.filter(array.indices, visible)).cast(pa.int64())
        dictionary_visible = pc.is_in(pa.array(range(len(array.dictionary)), type=pa.int64()),
                                      value_set=used)
        return pa.DictionaryArray.from_arrays(
            array.indices, convert_array(array.dictionary, dictionary_visible),
            ordered=data_type.ordered)
    if pa.types.is_union(data_type) or isinstance(data_type, pa.ExtensionType):
        raise ValueError('Unsupported Arrow type; refusing a potentially partial migration: '
                         + str(data_type))
    return array


def convert_table(table):
    """Convert all DATE32 fields, including nested fields and all chunks."""
    columns = [pa.chunked_array([convert_array(chunk) for chunk in column.chunks],
                                type=column.type) for column in table.columns]
    return pa.Table.from_arrays(columns, schema=table.schema)


def migrate(source, destination, file_format, confirmed_legacy=False):
    """Write a new file; source provenance must be confirmed by the caller."""
    if not confirmed_legacy:
        raise ValueError('Explicit confirmation of legacy Doris DATE encoding is required')
    source, destination = Path(source), Path(destination)
    if source.resolve() == destination.resolve():
        raise ValueError('Source and destination must be different files')
    if file_format not in ('orc', 'parquet'):
        raise ValueError('Format must be orc or parquet')
    # Convert one stripe/batch at a time to avoid loading the whole data file into memory.
    reader = orc.ORCFile(source) if file_format == 'orc' else parquet.ParquetFile(source)
    chunks = (reader.read_stripe(i) for i in range(reader.nstripes)) if file_format == 'orc' else (
        reader.iter_batches(batch_size=65536))
    schema = reader.schema if file_format == 'orc' else reader.schema_arrow
    if destination.exists():
        raise FileExistsError(destination)
    # Publish with an exclusive hard link only after the writer succeeds, including its footer.
    # An existence check alone races with another writer and could overwrite its output.
    temporary = tempfile.NamedTemporaryFile(prefix='.legacy-date-', dir=destination.parent,
                                           delete=False)
    temporary_path = Path(temporary.name)
    try:
        with temporary:
            writer = (orc.ORCWriter(temporary) if file_format == 'orc'
                      else parquet.ParquetWriter(temporary, schema))
            try:
                written = False
                for chunk in chunks:
                    table = (pa.Table.from_batches([chunk])
                             if isinstance(chunk, pa.RecordBatch) else chunk)
                    converted = convert_table(table)
                    if file_format == 'orc':
                        writer.write(converted)
                    else:
                        writer.write_table(converted)
                    written = True
                if not written:
                    empty = pa.Table.from_batches([], schema=schema)
                    if file_format == 'orc':
                        writer.write(empty)
                    else:
                        writer.write_table(empty)
            finally:
                writer.close()
            temporary.flush()
            os.fsync(temporary.fileno())
        os.link(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--format', required=True, choices=('orc', 'parquet'))
    parser.add_argument('--confirm-legacy-doris-date-encoding', action='store_true', required=True,
                        help='Confirm every DATE field in this file uses the old Doris encoding')
    args = parser.parse_args()
    migrate(args.input, args.output, args.format, args.confirm_legacy_doris_date_encoding)


if __name__ == '__main__':
    main()
