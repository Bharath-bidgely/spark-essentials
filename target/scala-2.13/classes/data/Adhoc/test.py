import pandas as pd
import csv
import sys
import os

def convert_excel_to_pipe_csv(input_file, output_file=None):
    """
    Convert Excel file to pipe-delimited CSV while preserving leading zeros.
    
    Parameters:
    input_file (str): Path to input Excel file
    output_file (str, optional): Path to output CSV file. If not provided, 
                                will use input filename with .csv extension
    """
    try:
        # Check if input file exists
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        # Read Excel file
        print(f"Reading Excel file: {input_file}")
        df = pd.read_excel(input_file)
        
        # If output file not specified, create name based on input file
        if output_file is None:
            output_file = os.path.splitext(input_file)[0] + '_output.csv'
        
        # Convert all columns to string type to preserve leading zeros
        df = df.astype(str)
        
        # Write to CSV with pipe delimiter
        df.to_csv(output_file, 
                  sep='|',              # Use pipe as delimiter
                  index=False,          # Don't write row numbers
                  quoting=csv.QUOTE_MINIMAL,  # Quote only when necessary
                  quotechar='"',        # Use double quotes for quoting
                  encoding='utf-8')     # UTF-8 encoding
        
        print(f"Successfully converted to CSV: {output_file}")
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    # Get input file path from user
    input_file = input("Please enter the path to your Excel file: ")
    
    # Get output file path (optional)
    output_file = input("Enter output file path (press Enter to use default): ").strip()
    if not output_file:
        output_file = None
    
    # Convert the file
    convert_excel_to_pipe_csv(input_file, output_file)
