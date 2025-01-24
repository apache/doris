import pandas as pd

def load_data(file_path):
    """
    Load data from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file.
    
    Returns:
        DataFrame: Loaded data as a pandas DataFrame.
    """
    try:
        data = pd.read_csv(file_path)
        print("Data loaded successfully.")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def analyze_data(data):
    """
    Perform basic data analysis.
    
    Args:
        data (DataFrame): Data to analyze.
    
    Returns:
        dict: Summary of the analysis.
    """
    summary = {
        "columns": data.columns.tolist(),
        "missing_values": data.isnull().sum().to_dict(),
        "description": data.describe().to_dict()
    }
    return summary

def save_summary(summary, output_file):
    """
    Save the summary report to a file.
    
    Args:
        summary (dict): Summary report to save.
        output_file (str): Path to the output file.
    """
    try:
        with open(output_file, 'w') as f:
            for key, value in summary.items():
                f.write(f"{key}:\n{value}\n\n")
        print(f"Summary saved to {output_file}.")
    except Exception as e:
        print(f"Error saving summary: {e}")

def main():
    # File paths
    input_file = "data.csv"  # Path to the input CSV file
    output_file = "summary.txt"  # Path to the output summary file
    
    # Load data
    data = load_data(input_file)
    if data is None:
        return
    
    # Analyze data
    summary = analyze_data(data)
    
    # Save summary
    save_summary(summary, output_file)

if __name__ == "__main__":
    main()
