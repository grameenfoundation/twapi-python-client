import os
from types import SimpleNamespace
import pandas as pd
from urllib.parse import quote
from lib.shared_utilities import get_pandas_dataframe_from_json_web_call, get_version_changelog_from_form_name, upload_payload_to_url, get_all_questions_in_org_then_filter
import xlsxwriter
import openpyxl
import tabulate
import re
import random
import json
import string
import html
from datetime import datetime

"""## Read excel file"""

def func_read_excel_file_and_upload(url_to_query,salesforce_service_url,auth_header,workingDirectory, fileName):
        
        # Read excel file into dataframes
        xls = pd.ExcelFile(os.path.join(workingDirectory,fileName))
        upload_form_dataframe = pd.read_excel(xls, 'Forms',header=0)
        upload_questions_without_options = pd.read_excel(xls, 'Questions', header=0)
        upload_options = pd.read_excel(xls, 'Options', header=0)
        upload_question_mapping = pd.read_excel(xls, 'Question_Mappings', header=0)
        upload_field_mapping_no_question_mapping = pd.read_excel(xls, 'Field_Mappings', header=0)
        upload_skip_logic = pd.read_excel(xls, 'Skip_Logic', header=0)
        upload_orm = pd.read_excel(xls, 'Object_Relationship_Mappings', header=0)

        form_result, form_name_to_upload = func_upload_form(url_to_query,salesforce_service_url,auth_header,upload_form_dataframe)
        existing_questions_lookup, existing_options_lookup, questions_without_options = func_fetch_existing_questions(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        parents_result = func_upload_questions_with_or_without_options(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, None, upload_questions_without_options, None, existing_questions_lookup,uploading_child_questions_not_parents=False)
        existing_questions_lookup, existing_options_lookup, questions_without_options = func_fetch_existing_questions(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)
        questions_result = func_upload_questions_with_or_without_options(url_to_query,salesforce_service_url,auth_header,form_name_to_upload,upload_options, upload_questions_without_options, existing_options_lookup, existing_questions_lookup, uploading_child_questions_not_parents=True)
        questions_result = pd.concat([parents_result,questions_result])
        existing_questions_lookup, existing_options_lookup, questions_without_options = func_fetch_existing_questions(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)
        upload_skip_logic_referencing_new_ids, field_mapping_referencing_new_ids, question_mapping_referencing_new_ids = func_update_dependent_objects_from_spreadsheet(url_to_query,salesforce_service_url,auth_header,upload_question_mapping, existing_questions_lookup, upload_field_mapping_no_question_mapping, upload_skip_logic)
        field_mapping_without_questions, question_mapping_dataframe = func_read_existing_field_and_form_mappings(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)
        form_mapping_result = func_upload_field_and_form_mappings(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, question_mapping_referencing_new_ids, field_mapping_referencing_new_ids, question_mapping_dataframe, field_mapping_without_questions)
        upload_orm_with_replaced_id = func_read_back_field_and_form_mapping_ids(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_orm)
        orm_result = func_upsert_orm(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_orm_with_replaced_id)
        skip_logic_result = func_upload_skip_logic(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_skip_logic_referencing_new_ids, existing_questions_lookup, existing_options_lookup)
        func_print_all_statuses_after_upload(form_result, questions_result, form_mapping_result, orm_result, skip_logic_result, workingDirectory, fileName)        


"""## Form"""

def func_upload_form(url_to_query,salesforce_service_url,auth_header,upload_form_dataframe):

        upload_form_dataframe_relevant_columns = upload_form_dataframe[['name','alias','messageAfterSubmission','description']].fillna("")
        form_name_to_upload = str(upload_form_dataframe_relevant_columns.name[0])
        # Fetch latest form ID and changelog from the API
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        updating_existing_form = form_id
        if updating_existing_form:
            #update existing form
            print('Update existing form ' + form_id)
            upload_form_dataframe_relevant_columns['id'] = form_id
            formVersionString = '"formVersion": [{"versionid": "' + form_version_id + '","changeLogNumber": "' + changelog_number + '"}]'
        else:
            #create new form
            print('Creating new form')
            upload_form_dataframe_relevant_columns['id'] = ""
            formVersionString = '"formVersion": [{"versionid": "","changeLogNumber": ""}]'

        # Forms made on the UI will have a null external ID even if they're updating, so handle form external IDs outside of "is updating" logic
        if (form_external_id):
            upload_form_dataframe_relevant_columns['externalId'] = form_external_id
        else:
            #Use the name as the external ID if none specified
            upload_form_dataframe_relevant_columns['externalId'] = upload_form_dataframe_relevant_columns['name']
            


        #This will only upload 1 form
        upload_str = str(upload_form_dataframe_relevant_columns.T.astype(str).to_json(force_ascii=False)).replace('{"0":','{"records":[')[:-2] + ',' + formVersionString + '}]}'
        #print(upload_str)
        form_update_endpoint = salesforce_service_url + 'formdata/v1?objectType=PutFormData'
        form_result = upload_payload_to_url(url_to_query,salesforce_service_url,auth_header,form_update_endpoint, upload_str)
        return form_result, form_name_to_upload
"""## Fetch existing Questions"""

def func_fetch_existing_questions(url_to_query,salesforce_service_url,auth_header,form_name_to_upload):
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        question_dataframe, persistent_full_question_dataframe = get_all_questions_in_org_then_filter(url_to_query,salesforce_service_url,auth_header,form_version_id,None)
        options_dataframe = pd.DataFrame(columns=["externalId" , "id" , "name" , "position" , "caption", "questionId", "questionName" ])
        for index, frame in question_dataframe.iterrows():
            if (frame.options):
                questionId = frame.id
                questionName = frame['name']
            else:
                questionId = None
                questionName = None
            individual_option_df = pd.DataFrame(frame.options)
            individual_option_df['questionId'] = questionId
            individual_option_df['questionName'] = questionName
            options_dataframe = pd.concat([individual_option_df,options_dataframe])

        questions_without_options = question_dataframe.loc[:, question_dataframe.columns != 'options']
        #if updating_existing_form:
        existing_questions_lookup = questions_without_options[['externalId','id','name','type']]
        existing_options_lookup = options_dataframe[['externalId','id','name','questionId','questionName']]

        return existing_questions_lookup, existing_options_lookup, questions_without_options

"""## Format new questions/options

### For this script, assume the excel file has been unsquished (only has 1 column with a caption)
"""


def func_upload_questions_with_or_without_options(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_options, upload_questions_without_options, existing_options_lookup, existing_questions_lookup,uploading_child_questions_not_parents):
    if (uploading_child_questions_not_parents):
        upload_options_sanitized = upload_options.copy()
        for column in upload_options:
            if ('caption' in column):
                #Remove the language-specific suffix
                upload_options_sanitized = upload_options_sanitized.rename(columns={column:'caption'})

        # Treat externalID as optional; if it is not defined, use the name instead
        if (upload_options_sanitized.empty):
            upload_options_sanitized['externalId'] = None
        else:
            upload_options_sanitized['externalId'] = upload_options_sanitized.apply(lambda x: str(x['questionName'])[-8:] + str(x['position']) + re.sub( '(?<!^)(?=[A-Z])', '_', x['name'] ).lower()[-8:], axis=1) #Replace Capital Letters with "_(lowercase letter)" to prevent duplicates from salesforce IDs
        upload_options_sanitized = upload_options_sanitized[['name','position','caption','questionName','externalId']]
    upload_questions_sanitized = upload_questions_without_options.copy().fillna("")
    for column in upload_questions_sanitized:
        if ('caption' in column):
            #Remove the language-specific suffix
            upload_questions_sanitized = upload_questions_sanitized.rename(columns={column:'caption'})
    
    if (uploading_child_questions_not_parents):
        upload_options_sanitized = upload_options_sanitized.merge(existing_options_lookup[['id','externalId']],how="left",on="externalId").fillna("")

    # Fetch latest form ID and changelog from the API
    form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

    if (uploading_child_questions_not_parents):
    #Convert options df back into nested json
        options_associated_with_questions = upload_options['questionName'].drop_duplicates()
        options_associated_with_questions

    upload_questions_with_options = upload_questions_sanitized.copy()
    upload_questions_with_options['options'] = None

    # Treat externalID as optional; if it is not defined, use the name instead
    upload_questions_with_options['externalId'] = upload_questions_with_options['name']

    upload_questions_with_options = upload_questions_with_options.merge(existing_questions_lookup[['id','name']],how="left",on="name").fillna('')

    if (uploading_child_questions_not_parents):
        if (not options_associated_with_questions.empty):
                for questionName in options_associated_with_questions.items():
                    thisQuestionName = questionName[1]
                    upload_options_json = str(upload_options_sanitized[upload_options_sanitized['questionName'] == thisQuestionName][['externalId','id','name','position','caption']].to_json(orient='records',force_ascii=False))
                    #print(upload_options_json)
                    row_index = upload_questions_with_options.index[upload_questions_with_options['name'] == thisQuestionName ].tolist()[0]
                    #print(row_index)
                    upload_questions_with_options.at[row_index,'options']= json.loads(upload_options_json)

        upload_questions_with_options = upload_questions_with_options.merge( \
                existing_questions_lookup[['id','name']].rename(columns={'id':'parentId','name':'parentName'}),
                how="left",on="parentName")\
                .rename(columns={'parentId':'parent'}).fillna('')

    #Replace double quotes with single quotes just for dynamic ops, captions and hints - most of the time this will be fine
    upload_questions_with_options['dynamicOperation'] = upload_questions_with_options['dynamicOperation'].apply(html.escape)
    # upload_questions_with_options.dynamicOperation = upload_questions_with_options.dynamicOperation.str.replace('\n','&#10;').replace('\t','&#9').replace('"','&#34;').replace("'",'&#39;')
    #upload_questions_with_options['caption'] = upload_questions_with_options['caption'].apply(html.escape)
    # upload_questions_with_options.caption = upload_questions_with_options.caption.str.replace('\n','&#10;').replace('\t','&#9').replace('"','”').replace("'",'’')
    # upload_questions_with_options.caption = upload_questions_with_options.caption.str.slice(0,254)
    #upload_questions_with_options['hint'] = upload_questions_with_options['hint'].apply(html.escape)
    # upload_questions_with_options.hint = upload_questions_with_options.hint.str.replace('\n','&#10;').replace('\t','&#9').replace('"','”').replace("'",'’')
    # upload_questions_with_options.hint = upload_questions_with_options.hint.str.slice(0,254)
    

    just_parent_sections = upload_questions_with_options[(upload_questions_without_options['type'] == 'section') | (upload_questions_without_options['type'] == 'repeat') ].reindex()
    not_parent_sections = upload_questions_with_options[(upload_questions_without_options['type'] != 'section') & (upload_questions_without_options['type'] != 'repeat') ] .reindex()

    if (uploading_child_questions_not_parents):
        upload_questions_with_options = not_parent_sections
    else:
        upload_questions_with_options = just_parent_sections

    upload_questions_with_options['parent'] = ''
    upload_questions_with_options['controllingQuestion'] = ''
    upload_questions_with_options['repeatSourceValue'] = ''

    # In cases where the parent name is not blank but the parent is blank, this means that no ID already exists for the question parent (it hasn't been updated) - use the externalID
    if (uploading_child_questions_not_parents):
        parent_externalId_lookup = just_parent_sections[just_parent_sections['parentName'] == ''][['name','externalId','id']].rename(columns={'name':'parentName','externalId':'parentExternalId','id':'parentId'})
        upload_questions_with_options = upload_questions_with_options.merge(parent_externalId_lookup,how="left",on='parentName').fillna('')
        upload_questions_with_options['parent'] = upload_questions_with_options.apply(lambda x: str(x.parentId) if x.parentId and not x.parent else x.parent, axis = 1)

        controlling_question_externalId_lookup = upload_questions_with_options[['name','externalId']].rename(columns={'name':'controllingQuestionName','externalId':'controllingQuestionExternalId'})
        upload_questions_with_options = upload_questions_with_options.merge(controlling_question_externalId_lookup,how="left",on='controllingQuestionName').fillna('')
        upload_questions_with_options['controllingQuestion'] = upload_questions_with_options.apply(lambda x: str(x.controllingQuestionExternalId) if x.controllingQuestionName and not x.controllingQuestion else x.controllingQuestion, axis = 1)

        repeat_source_externalId_lookup = upload_questions_with_options[['name','externalId']].rename(columns={'name':'repeatSourceValueName','externalId':'repeatSourceValueExternalId'})
        upload_questions_with_options = upload_questions_with_options.merge(repeat_source_externalId_lookup,how="left",on='repeatSourceValueName').fillna('')
        upload_questions_with_options['repeatSourceValue'] = upload_questions_with_options.apply(lambda x: str(x.repeatSourceValueExternalId) if x.repeatSourceValueName and not x.repeatSourceValue else x.repeatSourceValue, axis = 1)

    questions_result = None
    # Optimization - only upload 10 questions at a time
    for currentChunk in range(0, len(upload_questions_with_options.index), 10):

            # Fetch latest form ID and changelog from the API
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        upload_questions_with_options['form'] = form_id
        upload_questions_with_options['formVersion'] = form_version_id
        upload_questions_with_options['changeLogNumber'] = changelog_number
        minimum = max(currentChunk,0)
        maximum = min(currentChunk+10,len(upload_questions_with_options.index))


        question_with_options_creation_string = '{"records":' + str(upload_questions_with_options[['externalId', 'id', 'name',
            'caption', 'cascadingLevel',
            'cascadingSelect', 'controllingQuestion', 'displayRepeatSectionInTable','dynamicOperation',
            'dynamicOperationType', 'exampleOfValidResponse',
            'form', 'formVersion', 'hidden', 
            'maximum',  'minimum', 'parent', 'position',
            'previousVersion', 'printAnswer',
            'repeatSourceValue', 'repeatTimes',
            'required', 'responseValidation', 'showAllQuestionOnOnePage',
            'skipLogicBehavior', 'skipLogicOperator', 'hint',
            'testDynamicOperation', 'type', 'useCurrentTimeAsDefault', 'options',
            'changeLogNumber']].iloc[minimum:maximum].to_json(orient="records",force_ascii=False))\
            .replace("\\'","'").replace(',"options":""',',"options":[]')\
            .replace('"maximum":"",','').replace('"minimum":"",','').replace('"responseValidation":"",','').replace('"exampleOfValidResponse":"",','') + '}'
            # question_with_options_creation_string
        questions_temp_result =  upload_payload_to_url(url_to_query,salesforce_service_url,auth_header,salesforce_service_url + 'questiondata/v1?objectType=PutQuestionData', question_with_options_creation_string)
        
        if (questions_result is None):
                    questions_result = questions_temp_result
        else:
            questions_result = pd.concat([questions_result,questions_temp_result])
    return questions_result

"""### Update any dependent objects from the spreadsheet:


*   Question Mapping "question" field
*   Field Mapping "repeat" field
*   Skip Logic "sourceQuestion" and "parentQuestion"  
"""

def func_update_dependent_objects_from_spreadsheet(url_to_query,salesforce_service_url,auth_header,upload_question_mapping, existing_questions_lookup, upload_field_mapping_no_question_mapping, upload_skip_logic):
        question_id_lookup = existing_questions_lookup[['id','name']].rename(columns={'id':'questionId','name':'questionName'})
        question_mapping_referencing_new_ids = upload_question_mapping.merge(question_id_lookup,how="left").rename(columns={"questionId":'question'})

        contains_repeat_sections = upload_field_mapping_no_question_mapping[pd.isna(upload_field_mapping_no_question_mapping['repeatQuestionName']) == False]

        if (not contains_repeat_sections.empty):
            field_mapping_referencing_new_ids = upload_field_mapping_no_question_mapping.merge(question_id_lookup,how="left",left_on='repeatQuestionName',right_on="questionName").rename(columns={'questionId':'repeat'})
        else:
            upload_field_mapping_no_question_mapping['repeat'] = ''
            field_mapping_referencing_new_ids = upload_field_mapping_no_question_mapping

        upload_skip_logic_referencing_new_ids = upload_skip_logic.merge(question_id_lookup,how="left",left_on="sourceQuestionName",right_on="questionName")\
            .rename(columns={"questionId":"sourceQuestion"})\
            .drop(columns=['questionName'])\
            .merge(question_id_lookup,how="left",left_on="parentQuestionName",right_on="questionName")\
            .rename(columns={"questionId":"parentQuestion"}).drop(columns=['questionName'])
        
        return upload_skip_logic_referencing_new_ids, field_mapping_referencing_new_ids, question_mapping_referencing_new_ids

"""## Read existing Field and Form Mappings"""
def func_read_existing_field_and_form_mappings(url_to_query,salesforce_service_url,auth_header,form_name_to_upload):
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        field_mapping_endpoint = salesforce_service_url + "formmappingdata/v1?objectType=GetFormMappingData&formVersionId=" + form_version_id
        field_mapping_dataframe = pd.DataFrame(columns = ['externalId', 'id', 'name', 'form', 'formVersion',\
            'formVersionMappingField', 'mobileUserField', 'objectApiName',\
            'formMappingField', 'isReference', 'matchingField', 'repeat',\
            'submissionAPIField', 'changeLogNumber', 'questionMappings'])
        field_mapping_dataframe = pd.concat([field_mapping_dataframe,get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,field_mapping_endpoint, auth_header)])
        #Iterate all form mappings that have question mappings and create a new dataframe that has just the question mappings
        question_mapping_dataframe = pd.DataFrame(columns=["externalId", "name", "id", "fieldAPIName","isBroken","question","scoringGroup"])
        for index, frame in field_mapping_dataframe.iterrows():
            # if (frame.questionMappings):
            #     print(str(frame.questionMappings).replace('\'','"'))
            field_mapping_id = frame.id
            #JSON is case-sensitive, python apparently converts it into uppercase
            individual_question_mapping_df = pd.DataFrame(frame.questionMappings) 
            individual_question_mapping_df['field_mapping_id'] = field_mapping_id
            question_mapping_dataframe = pd.concat([individual_question_mapping_df,question_mapping_dataframe])
        field_mapping_without_questions = field_mapping_dataframe.loc[:, field_mapping_dataframe.columns != 'questionMappings']
        return field_mapping_without_questions, question_mapping_dataframe

"""## Field and Form Mapping"""

def func_upload_field_and_form_mappings(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, question_mapping_referencing_new_ids, field_mapping_referencing_new_ids, question_mapping_dataframe, field_mapping_without_questions):

        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        #Convert options df back into nested json
        question_mapping_associated_with_field_mapping = question_mapping_referencing_new_ids['fieldMappingName'].drop_duplicates()

        upload_field_mapping_with_question_mapping = field_mapping_referencing_new_ids.copy()
        upload_field_mapping_with_question_mapping['questionMappings'] = None

        if (upload_field_mapping_with_question_mapping.empty):
            upload_field_mapping_with_question_mapping['externalId'] = None
        else:
            upload_field_mapping_with_question_mapping['externalId'] = upload_field_mapping_with_question_mapping['name']

        if (question_mapping_referencing_new_ids.empty):
            question_mapping_referencing_new_ids['externalId'] = None
        else:
            question_mapping_referencing_new_ids['externalId'] = question_mapping_referencing_new_ids['name'] + question_mapping_referencing_new_ids['fieldAPIName'] 

        upload_question_mapping_with_ids = question_mapping_referencing_new_ids.merge(question_mapping_dataframe[['name','id']],how="left",on="name")
        if (not question_mapping_associated_with_field_mapping.empty):
            for field_mapping_name in question_mapping_associated_with_field_mapping.items():
                thisFieldMapping = field_mapping_name[1]
                # print(thisFieldMapping)
                upload_question_mapping_json = str(upload_question_mapping_with_ids[upload_question_mapping_with_ids['fieldMappingName'] == thisFieldMapping][['externalId', 'name', 'id', 'fieldAPIName', 'isBroken','question', 'scoringGroup']].to_json(orient='records',force_ascii=False))
                # print(upload_question_mapping_json)
                row_index = upload_field_mapping_with_question_mapping.index[upload_field_mapping_with_question_mapping['name'] == thisFieldMapping ].tolist()[0]
                # print(row_index)
                upload_field_mapping_with_question_mapping.at[row_index,'questionMappings']= upload_question_mapping_json

        upload_field_mapping_with_question_mapping['form'] = form_id
        upload_field_mapping_with_question_mapping['formVersion'] = form_version_id
        upload_field_mapping_with_question_mapping['changeLogNumber'] = changelog_number

        upload_field_mapping_with_question_mapping = upload_field_mapping_with_question_mapping.merge(field_mapping_without_questions[['id','name']],how="left",on="name")

        upload_field_mapping_with_question_mapping = upload_field_mapping_with_question_mapping.fillna("")

        upload_field_mapping_string = '{"records":' + str(upload_field_mapping_with_question_mapping[['externalId', 'id', 'name', 'form', 'formVersion',
            'formVersionMappingField', 'mobileUserField', 
            'objectApiName', 'formMappingField', 
            'isReference', 'matchingField', 'repeat', 
            'submissionAPIField', 'changeLogNumber', 'questionMappings']].astype(str).to_json(orient="records",force_ascii=False)).replace('\\','')\
            .replace('"[{"','[{"').replace(']"}',']}') + "}"
        # print(upload_field_mapping_string)

        if (upload_field_mapping_with_question_mapping.empty):
            form_mapping_result = "No Form Mapping to upload"
        else:
            form_mapping_result = upload_payload_to_url(url_to_query,salesforce_service_url,auth_header,salesforce_service_url + 'formmappingdata/v1?objectType=PutFormMappingData', upload_field_mapping_string)

        return form_mapping_result
"""## Read back field and form mapping IDs, update ORM IDs"""

def func_read_back_field_and_form_mapping_ids(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_orm):
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        field_mapping_endpoint = salesforce_service_url + "formmappingdata/v1?objectType=GetFormMappingData&formVersionId=" + form_version_id
        field_mapping_after_upload_dataframe = pd.DataFrame(columns = ["externalId","id" ,"name" ,"form" ,"formVersion" ,"formVersionMappingField" ,"mobileUserField" ,"patScoreMappingField" ,"objectApiName" ,"formMappingField" ,"intervieweeMapField" ,"isReference" ,"matchingField" ,"repeat" ,"useAsInterviewee" ,"submissionAPIField" ,"changeLogNumber" ,"questionMappings" ])
        field_mapping_after_upload_dataframe = pd.concat([field_mapping_after_upload_dataframe,get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,field_mapping_endpoint,auth_header)])

        upload_orm_with_replaced_id = upload_orm.merge(field_mapping_after_upload_dataframe.rename(columns={"name":"parentSurveyName","id":"parentSurveyMapping"})[['parentSurveyName','parentSurveyMapping']],how="left",on="parentSurveyName")\
            .merge(field_mapping_after_upload_dataframe.rename(columns={"name":"childSurveyName","id":"childSurveyMapping"})[['childSurveyName','childSurveyMapping']],how="left",on="childSurveyName")\
            [['name','fieldApiName','parentSurveyMapping','childSurveyMapping']]

        if (upload_orm_with_replaced_id.empty):
            upload_orm_with_replaced_id['externalId'] = None
        else:
            upload_orm_with_replaced_id['externalId'] = upload_orm_with_replaced_id['name']

        return upload_orm_with_replaced_id

"""## Update ORM"""

def func_upsert_orm(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_orm_with_replaced_id):
        # Fetch latest form ID and changelog from the API
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        #Fetch existing ORM
        orm_endpoint = salesforce_service_url + "questiondata/v1?objectType=GetObjectRelationshipMappingData&formVersionId=" + form_version_id
        orm_dataframe = pd.DataFrame(columns=["externalId" ,"id" ,"name" ,"fieldApiName" ,"parentSurveyMapping" ,"childSurveyMapping" ,"formVersion" ,"changeLogNumber"])
        orm_dataframe = pd.concat([orm_dataframe, get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,orm_endpoint,auth_header)])

        upload_orm_with_replaced_id = upload_orm_with_replaced_id.merge(orm_dataframe[['id','name']],how="left",on="name")

        upload_orm = upload_orm_with_replaced_id[['externalId', 'id', 'name', 'fieldApiName',
            'parentSurveyMapping', 'childSurveyMapping']].fillna("")

        upload_orm['formVersion'] = form_version_id
        upload_orm['changeLogNumber'] = changelog_number

        string_to_upload = '{"records":' + upload_orm.astype(str).to_json(orient="records",force_ascii=False) + '}'
        string_to_upload

        orm_update_endpoint = salesforce_service_url + 'objectrelationshipmappingdata/v1?objectType=PutObjectRelationshipMappingData'
        if (upload_orm.empty):
            orm_result = "No ORM to Upload"
        else:
            orm_result = upload_payload_to_url(url_to_query,salesforce_service_url,auth_header,orm_update_endpoint, string_to_upload)

        return orm_result

"""## Upload Skip Logic"""

def func_upload_skip_logic(url_to_query,salesforce_service_url,auth_header,form_name_to_upload, upload_skip_logic_referencing_new_ids, existing_questions_lookup, existing_options_lookup):
        # Fetch latest form ID and changelog from the API
        form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_upload)

        #Fetch existing Skip Logic
        skip_logic_endpoint = salesforce_service_url + "questiondata/v1?objectType=GetSkipLogicData&formVersionId=" + form_version_id
        skip_logic_dataframe = pd.DataFrame(columns=["externalId" ,"id" ,"negate" ,"skipValue" ,"condition" ,"parentQuestion" ,"sourceQuestion" ,"form" ,"formVersion" ,"changeLogNumber"])
        skip_logic_dataframe = pd.concat([skip_logic_dataframe, get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,skip_logic_endpoint, auth_header)])

        # Lookup the options that may be associated with this skip logic
                                                 
        # 1. create join column 'questionName', 'optionName'
        existing_options_lookup['optionsJoinColumn'] = existing_options_lookup['questionName'] +'-' + existing_options_lookup['name']
        # 2. create join column for skip logic of 'source_question_name', 'skipvaluename'
        upload_skip_logic_referencing_new_ids['optionsJoinColumn'] = upload_skip_logic_referencing_new_ids['sourceQuestionName'] + '-' + upload_skip_logic_referencing_new_ids['skipValueName']
        
        # 3. join on join column, add extra column to skip logic
        upload_skip_logic_referencing_new_ids = upload_skip_logic_referencing_new_ids.merge(existing_options_lookup[['id','optionsJoinColumn']].rename(columns={'id':'optionId'}),how="left",on="optionsJoinColumn")

        # 4. use lambda to replace if this exists, ignore if not
        upload_skip_logic_referencing_new_ids['skipValue'] = upload_skip_logic_referencing_new_ids.apply(lambda x: str(x['optionId']) if (x['optionId'] != '' and not pd.isnull(x['optionId'])) else ('' if pd.isnull(x['skipValueName']) else x['skipValueName']), axis=1)
        
        skip_logic_dataframe['joinColumn'] = skip_logic_dataframe['parentQuestion'] + '-' + skip_logic_dataframe['sourceQuestion'] + '-' + skip_logic_dataframe['skipValue']

        upload_skip_logic_referencing_new_ids['joinColumn'] = upload_skip_logic_referencing_new_ids['parentQuestion'] + '-' + upload_skip_logic_referencing_new_ids['sourceQuestion'] + '-' + upload_skip_logic_referencing_new_ids['skipValue']

        # Get existing IDs and external IDs for any existing skip logic
        upload_skip_logic_referencing_new_ids_joined = upload_skip_logic_referencing_new_ids.merge(skip_logic_dataframe[['id','externalId','joinColumn']],how="left",on="joinColumn").fillna("")

        if (upload_skip_logic_referencing_new_ids_joined.empty):
            upload_skip_logic_referencing_new_ids_joined['externalId'] = None
            return None
        else:
            upload_skip_logic_referencing_new_ids_joined['externalId'] = upload_skip_logic_referencing_new_ids_joined.apply(lambda x: str(x['externalId']) if x['externalId'] else x['joinColumn'], axis=1)

        
        upload_skip_logic = upload_skip_logic_referencing_new_ids_joined[['externalId', 'id', 'negate', 'skipValue', 'condition',
                    'parentQuestion', 'sourceQuestion']]

        upload_skip_logic['form'] = form_id
        upload_skip_logic['formVersion'] = form_version_id
        upload_skip_logic['changeLogNumber'] = changelog_number

        string_to_upload = '{"records":' + upload_skip_logic.astype(str).to_json(orient="records",force_ascii=False) + '}'

        form_update_endpoint = salesforce_service_url + 'skiplogicdata/v1/?objectType=PutSkipLogicData'
        if (upload_skip_logic.empty):
            skip_logic_result = "No Skip Logic to Upload"
        else:
            skip_logic_result = upload_payload_to_url(url_to_query,salesforce_service_url,auth_header,form_update_endpoint, string_to_upload)

        return skip_logic_result
"""# Review any errors"""

def func_print_all_statuses_after_upload(form_result, questions_result, form_mapping_result, orm_result, skip_logic_result, destination_folder, file_name):
     
     pd.set_option('display.max_colwidth', None)
     error_output = pd.DataFrame()

     if not type(form_result) is str:

        print ("Form")
        print(form_result.to_markdown())
        form_result_failures = form_result[form_result['success'] != True] 
        if (not form_result_failures.empty):
            print ("Form Failures")
            print(form_result_failures.to_markdown())
            error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Form Errors:"]})])
            error_output = pd.concat([error_output, form_result_failures]) 

     if not type(questions_result) is str:
        print ("Questions")
        print(questions_result.to_markdown())
        questions_result_failures = questions_result[questions_result['success'] != True] if not  type(questions_result) is str else pd.DataFrame()
        if (not questions_result_failures.empty): 
            print("Question Failures")
            print(questions_result_failures.to_markdown())
            error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Question Errors:"]})])
            error_output = pd.concat([error_output, questions_result_failures]) 
     if not type(form_mapping_result) is str:
        print("Form Mapping")
        print(form_mapping_result.to_markdown())
        form_mapping_result_failures = form_mapping_result[form_mapping_result['success'] != True] if not  type(form_mapping_result) is str else pd.DataFrame()
        if (not form_mapping_result_failures.empty): 
            print ("Form Mapping Failures")
            print(form_mapping_result_failures.to_markdown())
            error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Form Mapping Errors:"]})])
            error_output = pd.concat([error_output, form_mapping_result_failures]) 
     if not type(orm_result) is str:
        print("ORM")
        print(orm_result.to_markdown())
        orm_result_failures = orm_result[orm_result['success'] != True] if not  type(orm_result) is str else pd.DataFrame()
        if (not orm_result_failures.empty):
            print ("ORM Failures")
            print(orm_result_failures.to_markdown())
            error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Object Relationship Mapping Errors:"]})])
            error_output = pd.concat([error_output, orm_result_failures]) 
    # NOTE: Bug IDALMSA-12051 causes the API to return "Skip Condition created successfully" when the API has actually updated instead of created. Low priority to fix as this doesn't break anything.
     if not type(skip_logic_result) is str:
        print("Skip Logic")
        print(skip_logic_result.to_markdown())
        skip_logic_result_failures = skip_logic_result[skip_logic_result['success'] != True] if not  type(skip_logic_result) is str else pd.DataFrame()
        if (not skip_logic_result_failures.empty):
            print ("Skip Logic Failures")
            print(skip_logic_result_failures.to_markdown())
            error_output = pd.concat([error_output, pd.DataFrame({'message':[ "Skip Logic Upload Errors:"]})])
            error_output = pd.concat([error_output, skip_logic_result_failures]) 

     if (not error_output.empty):
        error_output = pd.concat([error_output, pd.DataFrame({'message':[ 'File with errors' + file_name ]})])
        error_output = pd.concat([error_output, pd.DataFrame({'message':[ 'Time: ' + datetime.today().strftime('%Y-%m-%d %H:%M:%S') ]})])
        error_writer = pd.ExcelWriter(os.path.join(destination_folder,"error_" + file_name), engine='xlsxwriter')
        error_output.to_excel(error_writer, sheet_name="errors", index=False, startrow=0, startcol=0)
        error_writer.close()


def upload_all_files_in_folder(url_to_query,salesforce_service_url,auth_header,workingDirectory):
    for filename in os.listdir(workingDirectory):
        f = os.path.join(workingDirectory, filename)
        if os.path.isfile(f):
            print(f)
            func_read_excel_file_and_upload(url_to_query,salesforce_service_url,auth_header,workingDirectory, filename)