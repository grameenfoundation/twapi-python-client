import os
import glob
import pandas as pd
import xlsxwriter
import shutil

def find_and_combine_translations(source_folder, destination_folder, squished_form_prefix):
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
            print(files)
            squishFiles(files, source_folder, destination_folder,squished_form_prefix)

def squishFiles(files, source_folder, destination_folder,squished_form_prefix):
    base_name = files[0].split('_')[0]
    squished_file_name = f"{squished_form_prefix}{base_name}_squished.xlsx"

    error_output = pd.DataFrame()
    
    sheets = ["Forms", "Questions", "Options", "Question_Mappings", "Field_Mappings", "Skip_Logic", "Object_Relationship_Mappings"]
    writer = pd.ExcelWriter(os.path.join(destination_folder,squished_file_name), engine='xlsxwriter')
    # Loop through each sheet and combine the data from all files for that sheet
    for sheet in sheets:
        language_not_important_data = pd.DataFrame()
        language_specific_data = pd.DataFrame()
        for file in files:
            # Extract the language from the file name
            language = file.split('_')[-2]
            
            # Open the Excel file using pandas
            xl = pd.ExcelFile(os.path.join(source_folder,file))
            
            # Get the data for the current sheet
            sheet_data = pd.read_excel(xl, sheet_name=sheet,header=0)
            warning_has_been_thrown = False
            for column in sheet_data.columns:

                    combined_data_length = len(language_not_important_data.index)
                    new_data_length = len(sheet_data.index)
                    if ((warning_has_been_thrown == False) and (combined_data_length > 0) and (combined_data_length != new_data_length)):
                        error_output = pd.concat([error_output, pd.DataFrame({'message':[ "WARNING - data length mismatch. This likely means your translated forms have incompatible configurations"]})])
                        combined_column_to_compare = language_not_important_data.iloc[:,0]
                        sheet_data_to_compare = sheet_data.iloc[:,0]
                        if (sheet == "Options"):
                            combined_column_to_compare = language_not_important_data[['questionName','position']]
                            sheet_data_to_compare = sheet_data[['questionName','position']]
                        
                        if (sheet == "Question_Mappings"):
                            combined_column_to_compare = language_not_important_data[['questionName','fieldAPIName']]
                            sheet_data_to_compare = sheet_data[['questionName','fieldAPIName']]
                        
                        difference = pd.concat([combined_column_to_compare,sheet_data_to_compare]).drop_duplicates(keep=False)
                        error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Difference:"]})])
                        error_output = pd.concat([error_output, difference]) 
                        error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Current data:"]})])
                        error_output = pd.concat([error_output, language_not_important_data])
                        error_output = pd.concat([error_output, pd.DataFrame({'message':[ "New data that is incompatible:"]})])
                        error_output = pd.concat([error_output, sheet_data])
                        warning_has_been_thrown = True
                    # Append the language column to the sheet data

                    new_name = column
                    if ('::' in column):
                        new_name = column.split('::')[0] + "::" + language

                        # Add the prefix if desired (only for form name/alias)
                        if ((squished_form_prefix != '') and (sheet == 'Forms' and (column.split('::')[0] == 'name') or (column.split('::')[0] == 'alias'))):
                            language_specific_data[new_name] = squished_form_prefix + sheet_data[column]
                        else:
                            language_specific_data[new_name] = sheet_data[column]

                    else:
                        language_not_important_data[new_name] = sheet_data[column]
                    
        # Concatenate horizontally for language-dependent and language-independent dataframes
        combined_data = pd.concat([language_not_important_data, language_specific_data], axis=1)

        
        # Write the combined data for this sheet to a new sheet in the squished file
        combined_data.to_excel(writer, sheet_name=sheet, index=False, startrow=0, startcol=0)
    print(f"Squished file written to {squished_file_name}")
    writer.close()

    if (not error_output.empty):
        error_writer = pd.ExcelWriter(os.path.join(destination_folder,"error_" + squished_file_name), engine='xlsxwriter')
        error_output.to_excel(error_writer, sheet_name="errors", index=False, startrow=0, startcol=0)
        error_writer.close()