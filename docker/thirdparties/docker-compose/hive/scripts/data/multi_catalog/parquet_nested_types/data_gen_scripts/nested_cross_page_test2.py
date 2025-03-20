import pyarrow as pa
import pyarrow.parquet as pq
import subprocess
import json
import argparse

# Define the output file path as a constant
OUTPUT_PARQUET_FILE = 'nested_cross_page_test2.parquet'

def generate_cross_page_test_data(output_file):
    # Create test data
    data = {
        # id column (INT32)
        'id': [1, 2, 3],
        
        # Multi-level array column (ARRAY<ARRAY<INT>>)
        'nested_array_col': [
            # Row 1 - Large array
            [[i for i in range(10)] for _ in range(100)],  # 100 sub-arrays, each with 10 elements
            
            # Row 2 - Small array
            [[1, 2], [3, 4], [5, 6]],
            
            # Row 3 - Another large array
            [[i for i in range(10, 20)] for _ in range(100)]
        ],
        
        # Struct array column (ARRAY<STRUCT<x: int, y: string>>)
        'array_struct_col': [
            # Row 1
            [{'x': i, 'y': f'value_{i}'} for i in range(500)],
            
            # Row 2
            [{'x': 1, 'y': 'small'}, {'x': 2, 'y': 'array'}],
            
            # Row 3
            [{'x': i, 'y': f'big_{i}'} for i in range(500)]
        ],
        
        # Map column (MAP<STRING, ARRAY<INT>>)
        'map_array_col': [
            # Row 1
            {f'key_{i}': list(range(i, i+10)) for i in range(50)},
            
            # Row 2
            {'small_key': [1, 2, 3]},
            
            # Row 3
            {f'big_key_{i}': list(range(i*10, (i+1)*10)) for i in range(50)}
        ],
        
        # Complex nested structure (STRUCT<
        #   a: ARRAY<INT>, 
        #   b: MAP<STRING, ARRAY<INT>>,
        #   c: STRUCT<x: ARRAY<INT>, y: STRING>
        # >)
        'complex_struct_col': [
            # Row 1
            {
                'a': list(range(100)),
                'b': {f'key_{i}': list(range(i, i+5)) for i in range(20)},
                'c': {'x': list(range(50)), 'y': 'nested_struct_1'}
            },
            
            # Row 2
            {
                'a': [1, 2, 3],
                'b': {'small': [1, 2]},
                'c': {'x': [1], 'y': 'small_struct'}
            },
            
            # Row 3
            {
                'a': list(range(100, 200)),
                'b': {f'big_{i}': list(range(i*5, (i+1)*5)) for i in range(20)},
                'c': {'x': list(range(50)), 'y': 'nested_struct_2'}
            }
        ],
        
        # Description column (STRING)
        'description': [
            'Row with large nested arrays and structures',
            'Row with small nested data',
            'Row with another set of large nested arrays and structures'
        ]
    }
    
    # Create complex table structure
    table = pa.Table.from_pydict({
        'id': pa.array(data['id'], type=pa.int32()),
        
        # Multi-level array type
        'nested_array_col': pa.array(data['nested_array_col'], 
                                   type=pa.list_(pa.list_(pa.int32()))),
        
        # Struct array type
        'array_struct_col': pa.array(data['array_struct_col'],
                                   type=pa.list_(pa.struct([
                                       ('x', pa.int32()),
                                       ('y', pa.string())
                                   ]))),
        
        # Map type
        'map_array_col': pa.array(data['map_array_col'],
                                type=pa.map_(pa.string(), pa.list_(pa.int32()))),
        
        # Complex nested structure type
        'complex_struct_col': pa.array(data['complex_struct_col'],
                                     type=pa.struct([
                                         ('a', pa.list_(pa.int32())),
                                         ('b', pa.map_(pa.string(), pa.list_(pa.int32()))),
                                         ('c', pa.struct([
                                             ('x', pa.list_(pa.int32())),
                                             ('y', pa.string())
                                         ]))
                                     ])),
        
        'description': pa.array(data['description'], type=pa.string())
    })
    
    # Write to parquet file
    pq.write_table(
        table,
        output_file,
        compression=None,  # No compression
        version='2.6',
        write_statistics=True,
        row_group_size=3,        # All data in one row group
        data_page_size=100,      # Very small page size
        write_batch_size=1       # Minimum batch size
    )

def inspect_parquet_file(file_path):
    """Inspect the structure of generated parquet file"""
    pf = pq.ParquetFile(file_path)
    print(f"\nFile: {file_path}")
    print(f"Number of row groups: {pf.num_row_groups}")
    
    metadata = pf.metadata
    schema = pf.schema
    print(f"\nSchema: {schema}")
    print(f"\nDetailed schema:")
    for i in range(len(schema)):
        print(f"Column {i}: {schema[i]}")
    
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        print(f"\nRow Group {i}:")
        print(f"Num rows: {rg.num_rows}")
        
        for j in range(rg.num_columns):
            col = rg.column(j)
            print(f"\nColumn {j}:")
            print(f"Path: {schema[j].name}")
            print(f"Type: {col.physical_type}")
            print(f"Encodings: {col.encodings}")
            print(f"Total compressed size: {col.total_compressed_size}")
            print(f"Total uncompressed size: {col.total_uncompressed_size}")
            print(f"Number of values: {col.num_values}")
            print(f"Data page offset: {col.data_page_offset}")
            if col.dictionary_page_offset is not None:
                print(f"Dictionary page offset: {col.dictionary_page_offset}")

def format_value(value):
    """Format value for printing"""
    if isinstance(value, (list, dict)):
        return f"{str(value)[:100]}... (length: {len(str(value))})"
    return str(value)

def read_and_print_file(file_path):
    """Read and print file content"""
    table = pq.read_table(file_path)
    df = table.to_pandas()
    print("\nFile content:")
    
    for i in range(len(df)):
        print(f"\nRow {i}:")
        for column in df.columns:
            value = df.iloc[i][column]
            print(f"{column}: {format_value(value)}")

def inspect_pages_with_cli(file_path, parquet_cli_path=None):
    """
    Inspect page information using parquet-cli
    
    Args:
        file_path: Path to the parquet file
        parquet_cli_path: Optional path to parquet-cli jar file
    """
    if not parquet_cli_path:
        print("\nSkipping parquet-cli inspection: No parquet-cli path provided")
        return

    print("\nParquet CLI Output:")
    try:
        cmd = f"java -jar {parquet_cli_path} pages {file_path}"
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running parquet-cli: {e}")
        if e.output:
            print(f"Error output: {e.output}")
    except Exception as e:
        print(f"Unexpected error running parquet-cli: {e}")

def save_test_data_info(output_file):
    """Save detailed test data information to text file"""
    info = {
        "file_format": "Parquet",
        "version": "2.6",
        "compression": "None",
        "row_group_size": 3,
        "data_page_size": 100,
        "write_batch_size": 1,
        "output_file": output_file,
        "schema": {
            "id": "INT32",
            "nested_array_col": "ARRAY<ARRAY<INT32>>",
            "array_struct_col": "ARRAY<STRUCT<x: INT32, y: STRING>>",
            "map_array_col": "MAP<STRING, ARRAY<INT32>>",
            "complex_struct_col": """STRUCT<
                a: ARRAY<INT32>,
                b: MAP<STRING, ARRAY<INT32>>,
                c: STRUCT<
                    x: ARRAY<INT32>,
                    y: STRING
                >
            >""",
            "description": "STRING"
        },
        "test_cases": [
            {
                "row": 1,
                "description": "Large nested data",
                "characteristics": [
                    "Large nested arrays (100 arrays of 10 elements each)",
                    "Large struct array (500 elements)",
                    "Large map (50 key-value pairs)",
                    "Complex nested structure with large arrays"
                ]
            },
            {
                "row": 2,
                "description": "Small nested data",
                "characteristics": [
                    "Small nested arrays (3 arrays of 2 elements each)",
                    "Small struct array (2 elements)",
                    "Small map (1 key-value pair)",
                    "Complex nested structure with small arrays"
                ]
            },
            {
                "row": 3,
                "description": "Another large nested data",
                "characteristics": [
                    "Large nested arrays (100 arrays of 10 elements each)",
                    "Large struct array (500 elements)",
                    "Large map (50 key-value pairs)",
                    "Complex nested structure with large arrays"
                ]
            }
        ]
    }
    
    info_file = output_file.replace('.parquet', '_info.json')
    with open(info_file, 'w') as f:
        json.dump(info, f, indent=2)

if __name__ == '__main__':
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description='Generate and inspect parquet test data')
    parser.add_argument('--parquet-cli', 
                       help='Path to parquet-cli jar file',
                       default=None)
    parser.add_argument('--output',
                       help='Output parquet file path',
                       default=OUTPUT_PARQUET_FILE)
    args = parser.parse_args()

    # Use the output file path from command line or default
    output_file = args.output

    generate_cross_page_test_data(output_file)
    inspect_parquet_file(output_file)
    read_and_print_file(output_file)
    inspect_pages_with_cli(output_file, args.parquet_cli)
    save_test_data_info(output_file)
