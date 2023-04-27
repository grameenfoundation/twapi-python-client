import http.client
import json
from types import SimpleNamespace
from urllib.parse import quote
import pandas as pd
import subprocess
import os
import glob

def reformat_dataframe_to_single_language(original_dataframe, language_to_use):
        new_dataframe = pd.DataFrame()
        for column_name in original_dataframe.columns:
              if ('::' in column_name):
                  language_name = column_name.split('::')[-1]
                  column_name_without_language = column_name.split('::')[0]
                  if (language_name == language_to_use):
                        new_dataframe[column_name_without_language] = original_dataframe[column_name]
              elif 'taroId' in column_name:
                  new_dataframe[column_name] = original_dataframe[column_name] + '::' + language_to_use
              elif 'Unnamed: 0' not in column_name:
                  # This column does not have '::', so is not language-specific
                  new_dataframe[column_name] = original_dataframe[column_name]
        return new_dataframe

def write_all_to_excel(destination_directory, excel_name, form_dataframe, question_dataframe, option_dataframe, question_mapping_dataframe, field_mapping_dataframe, skip_logic_dataframe, orm_dataframe):
      writer = pd.ExcelWriter(os.path.join(destination_directory,excel_name),engine='xlsxwriter')
      workbook=writer.book
      form_dataframe.to_excel(writer,sheet_name='Forms',startrow=1 , startcol=0)
      question_dataframe.to_excel(writer,sheet_name='Questions',startrow=1 , startcol=0)
      option_dataframe.to_excel(writer,sheet_name='Options',startrow=1 , startcol=0)
      question_mapping_dataframe.to_excel(writer,sheet_name='Question_Mappings',startrow=1 , startcol=0)
      field_mapping_dataframe.to_excel(writer,sheet_name='Field_Mappings',startrow=1 , startcol=0)
      skip_logic_dataframe.to_excel(writer,sheet_name='Skip_Logic',startrow=1 , startcol=0)
      orm_dataframe.to_excel(writer,sheet_name='Object_Relationship_Mappings',startrow=1 , startcol=0)
      writer.close()

def unsquish_file(source_directory,destination_directory,filename):
    xls = pd.ExcelFile(os.path.join(source_directory,filename))
    upload_form_dataframe = pd.read_excel(xls, 'Forms',header=1)
    upload_questions_without_options = pd.read_excel(xls, 'Questions', header=1)
    upload_options = pd.read_excel(xls, 'Options', header=1)
    upload_question_mapping = pd.read_excel(xls, 'Question_Mappings', header=1)
    upload_field_mapping_no_question_mapping = pd.read_excel(xls, 'Field_Mappings', header=1)
    upload_skip_logic = pd.read_excel(xls, 'Skip_Logic', header=1)
    upload_orm = pd.read_excel(xls, 'Object_Relationship_Mappings', header=1)
    languages_in_use = []

    for column_name in upload_form_dataframe.columns:
        if ('::' in column_name):
            language_name = column_name.split('::')[-1]
            if not language_name in languages_in_use:
                languages_in_use.append(language_name)  
    
    for language in languages_in_use:
        form_name = upload_form_dataframe['name::' + language][0]
        new_form_df = reformat_dataframe_to_single_language(upload_form_dataframe,language)
        # When working with a new form, set the change log to 0
        new_form_df['changeLog'] = 0
        new_form_df['taroId'] = form_name
        new_question_df = reformat_dataframe_to_single_language(upload_questions_without_options,language)
        new_options_df = reformat_dataframe_to_single_language(upload_options,language) 
        new_question_mapping_df = reformat_dataframe_to_single_language(upload_question_mapping,language) 
        new_field_mapping_df = reformat_dataframe_to_single_language(upload_field_mapping_no_question_mapping,language) 
        new_skip_logic_df = reformat_dataframe_to_single_language(upload_skip_logic,language) 
        new_orm_df = reformat_dataframe_to_single_language(upload_orm,language)
        write_all_to_excel(destination_directory,form_name,new_form_df,new_question_df,new_options_df,new_question_mapping_df,new_field_mapping_df,new_skip_logic_df,new_orm_df)

def unsquish_all_files_in_folder(source_directory,destination_directory):
    for file_path in glob.glob(os.path.join(source_directory, '*.xlsx')):
        # Get the file name
        file_name = os.path.basename(file_path)
        print(file_name)
        unsquish_file(source_directory,destination_directory,file_name)