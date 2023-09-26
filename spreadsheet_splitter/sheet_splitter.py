import pandas as pd
import os
import re

def split_excel_sheets(excel_filepath, output_folder):
    # Ensure the output folder exists; if not, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Load the Excel file
    xls = pd.ExcelFile(excel_filepath)

    # Regular expression pattern for the date format 'dd mmm yy'
    pattern = re.compile(r"^\d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{2}$")

    # Loop through each sheet in the Excel file
    for sheet_name in xls.sheet_names:
        # Check if the sheet name matches the date pattern
        if pattern.match(sheet_name):
            # Create a new filename for each sheet, based on the output folder
            new_filename = os.path.join(output_folder, f"{sheet_name}.xlsx")

            # Check if the file already exists in the folder
            if os.path.exists(new_filename):
                print(f"Sheet {sheet_name} already exists in the folder. Skipping.")
                continue

            df = pd.read_excel(xls, sheet_name)
            df.to_excel(new_filename, index=False, engine='openpyxl')
            print(f"Saved sheet {sheet_name} to {new_filename}")
        else:
            print(f"Skipping sheet {sheet_name} as it doesn't match the 'dd mmm yy' pattern.")

if __name__ == "__main__":
    filepath = input("Enter the path to the Excel file: ")
    output_folder = input("Enter the path to the output folder: ")
    split_excel_sheets(filepath, output_folder)
