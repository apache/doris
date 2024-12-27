import pyarrow as pa
import pyarrow.parquet as pq
import subprocess
import argparse
import json

# Define the output file path as a constant
OUTPUT_PARQUET_FILE = 'nested_cross_page_test1.parquet'

def generate_cross_page_test_data(output_file):
    # Create test data
    data = {
        # id column (INT32)
        'id': [1, 2, 3],
        
        # array column (ARRAY<INT>)
        'array_col': [
            # Row 1 - Large array to force cross-page
            [1, 2, 3, 4, 5] * 200,  # 1000 elements
            
            # Row 2 - Normal sized array
            [1, 2, 3],
            
            # Row 3 - Another large array
            [6, 7, 8, 9, 10] * 200  # 1000 elements
        ],
        
        # description column (STRING)
        'description': [
            'This is a large array with repeated sequence [1,2,3,4,5]',
            'This is a small array with just three elements',
            'This is another large array with repeated sequence [6,7,8,9,10]'
        ]
    }
    
    # Create table structure
    table = pa.Table.from_pydict({
        'id': pa.array(data['id'], type=pa.int32()),
        'array_col': pa.array(data['array_col'], type=pa.list_(pa.int32())),
        'description': pa.array(data['description'], type=pa.string())
    })
    
    # Write to parquet file
    pq.write_table(
        table,
        output_file,
        compression=None,  # No compression for predictable size
        version='2.6',
        write_statistics=True,
        row_group_size=3,        # All data in one row group
        data_page_size=100,      # Very small page size
        write_batch_size=10      # Small batch size
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

def read_and_print_file(file_path):
    """Read and print file content"""
    table = pq.read_table(file_path)
    df = table.to_pandas()
    print("\nFile content:")
    for i in range(len(df)):
        print(f"\nRow {i}:")
        print(f"ID: {df.iloc[i]['id']}")
        arr = df.iloc[i]['array_col']
        print(f"Array length: {len(arr)}")
        print(f"First few elements: {arr[:5]}...")
        print(f"Last few elements: ...{arr[-5:]}")
        print(f"Description: {df.iloc[i]['description']}")

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
        "write_batch_size": 10,
        "output_file": output_file,
        "schema": {
            "id": "INT32",
            "array_col": "ARRAY<INT32>",
            "description": "STRING"
        },
        "test_cases": [
            {
                "row": 1,
                "description": "Large array",
                "characteristics": [
                    "1000 elements",
                    "Repeated sequence [1,2,3,4,5]",
                    "Forces cross-page scenario"
                ]
            },
            {
                "row": 2,
                "description": "Small array",
                "characteristics": [
                    "3 elements",
                    "Simple sequence [1,2,3]",
                    "Fits in single page"
                ]
            },
            {
                "row": 3,
                "description": "Another large array",
                "characteristics": [
                    "1000 elements",
                    "Repeated sequence [6,7,8,9,10]",
                    "Forces cross-page scenario"
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
