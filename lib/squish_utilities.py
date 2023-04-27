import os
import glob
import pandas as pd
import xlsxwriter
import shutil

def find_and_combine_translations(source_folder, destination_folder):
    # Define the language codes to check for
    LANGUAGES = ['en', 'es', 'fr']
    # Create a dictionary to hold files with the same name
    file_dict = {}
    # Iterate through all files in the folder
    for file_path in glob.glob(os.path.join(source_folder, '*.xlsx')):
        # Get the file name
        file_name = os.path.basename(file_path)
        # Check if the file name has a language code
        if any(lang in file_name for lang in LANGUAGES):
            # Get the base file name
            base_name = file_name.split('_')[0]
            # Check if this file has a translation
            if base_name in file_dict:
                # Add this file to the list of translations
                file_dict[base_name].append(file_name)
            else:
                # Create a new list for this file
                file_dict[base_name] = [file_name]
    # Iterate through the files with translations and call the "squishFiles" function
    for name, files in file_dict.items():
        if len(files) > 1:
            print(files)
            squishFiles(files, source_folder, destination_folder)
        #otherwise just copy the file
        else:
            shutil.copyfile(os.path.join(source_folder,files[0]), os.path.join(destination_folder,files[0]))

def squishFiles(files, source_folder, destination_folder):
    base_name = files[0].split('_')[0]
    squished_file_name = f"{base_name}_squished.xlsx"
    
    sheets = ["Forms", "Questions", "Options", "Question_Mappings", "Field_Mappings", "Skip_Logic", "Object_Relationship_Mappings"]
    writer = pd.ExcelWriter(os.path.join(destination_folder,squished_file_name), engine='xlsxwriter')
    # Loop through each sheet and combine the data from all files for that sheet
    for sheet in sheets:
        combined_data = pd.DataFrame()
        for file in files:
            # Extract the language from the file name
            language = file.split('_')[-2]
            
            # Open the Excel file using pandas
            xl = pd.ExcelFile(os.path.join(source_folder,file))
            
            # Get the data for the current sheet
            sheet_data = pd.read_excel(xl, sheet_name=sheet,header=1)
            for column in sheet_data.columns:
                    new_name = column
                    if ('::' in column):
                        new_name = column.split('::')[0] + "::" + language

                    # Append the language column to the sheet data
                    combined_data[new_name] = sheet_data[column]
            
        # Write the combined data for this sheet to a new sheet in the squished file
        
        combined_data.to_excel(writer, sheet_name=sheet, index=False, startrow=1, startcol=0)
    print(f"Squished file written to {squished_file_name}")
    writer.close()