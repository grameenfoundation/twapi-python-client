import pandas as pd
import xlsxwriter
from lib.shared_utilities import get_pandas_dataframe_from_json_web_call, get_version_changelog_from_form_name, upload_payload_to_url, get_all_questions_in_org_then_filter

def get_all_dataframes_and_write_to_excel_from_form_name(url_to_query,salesforce_service_url,auth_header,workingDirectory,form_name_to_download,persistent_full_question_dataframe = None):
    form_version_id, changelog_number, form_id, form_external_id, form_dataframe = get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name_to_download)
    
    # Get All Questions
    
    question_dataframe, persistent_full_question_dataframe = get_all_questions_in_org_then_filter(url_to_query,salesforce_service_url,auth_header,form_version_id,persistent_full_question_dataframe)
    
    if (question_dataframe.empty):
        print("No Questions in this form")
        return None
    #Iterate all questions that have options and create a new dataframe that has just the options
    options_dataframe = pd.DataFrame(columns=["externalId" , "id" , "name" , "position" , "caption","questionId" ])
    for index, frame in question_dataframe.iterrows():
        if (frame.options):
          questionId = frame.id
          individual_option_df = pd.read_json(str(frame.options).replace('\'','"'))
          individual_option_df['questionId'] = questionId
          options_dataframe = pd.concat([individual_option_df,options_dataframe])
    questions_without_options = question_dataframe.loc[:, question_dataframe.columns != 'options']
    parentLookup = questions_without_options[questions_without_options['parent'] == ""][['position','name','id']].rename(columns={'position':'parentPosition','name':'parentName','id':'parentId'})
    cascadingSelectLookup = questions_without_options[questions_without_options['type'] == "cascading-select"][['position','name','id']].rename(columns={'position':'parentPosition','name':'parentName','id':'parentId'})
    if (not cascadingSelectLookup.empty):
        parentLookup = pd.concat([parentLookup,cascadingSelectLookup])
    questions_with_order = questions_without_options.copy()
    hackyMultiplier = 10000 #arbitrarily large hacky multiplier
    print(parentLookup)
    print(questions_with_order[['id','name','parent','type']])
    questions_with_order['formOrder'] = \
        questions_with_order.apply(lambda x: 
            int(parentLookup[parentLookup['parentId'] == x['parent']]['parentPosition'].iloc[0]) * hackyMultiplier + int(x['position'])  \
                if x['parent'] != "" else int(x['position']) * hackyMultiplier, axis =1 )
    questions_without_options = questions_with_order.sort_values(by=['formOrder']).drop(columns=['formOrder'])
    # Get all field mappings
    field_mapping_endpoint = salesforce_service_url + "formmappingdata/v1?objectType=GetFormMappingData&formVersionId=" + form_version_id
    field_mapping_dataframe = pd.DataFrame(columns = ['externalId', 'id', 'name', 'form', 'formVersion',\
          'formVersionMappingField', 'mobileUserField', 'objectApiName',\
          'formMappingField', 'isReference', 'matchingField', 'repeat',\
          'submissionAPIField', 'changeLogNumber', 'questionMappings'])
    field_mapping_dataframe = pd.concat([field_mapping_dataframe,get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url, field_mapping_endpoint, auth_header)])
    #Iterate all form mappings that have question mappings and create a new dataframe that has just the question mappings
    question_mapping_dataframe = pd.DataFrame(columns=["externalId", "name", "id", "fieldAPIName","isBroken","question","scoringGroup","field_mapping_id"])
    for index, frame in field_mapping_dataframe.iterrows():
        if (frame.questionMappings):
          print(str(frame.questionMappings).replace('\'','"'))
          field_mapping_id = frame.id
          #JSON is case-sensitive, python apparently converts it into uppercase
          individual_question_mapping_df = pd.read_json(str(frame.questionMappings).replace('\'','"').replace("True","true").replace("False","false"))
          individual_question_mapping_df['field_mapping_id'] = field_mapping_id
          question_mapping_dataframe = pd.concat([individual_question_mapping_df,question_mapping_dataframe])
    field_mapping_without_questions = field_mapping_dataframe.loc[:, field_mapping_dataframe.columns != 'questionMappings']
    # Get all Skip Logic
    skip_logic_endpoint = salesforce_service_url + "skiplogicdata/v1?objectType=GetSkipLogicData&formVersionId=" + form_version_id
    skip_logic_dataframe = pd.DataFrame(columns=["externalId" ,"id" ,"negate" ,"skipValue" ,"condition" ,"parentQuestion" ,"sourceQuestion" ,"form" ,"formVersion" ,"changeLogNumber"])
    skip_logic_dataframe = pd.concat([skip_logic_dataframe, get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,skip_logic_endpoint, auth_header)])
    # Get all ORM
    orm_endpoint = salesforce_service_url + "objectrelationshipmappingdata/v1?objectType=GetObjectRelationshipMappingData&formVersionId=" + form_version_id
    orm_dataframe = pd.DataFrame(columns=["externalId" ,"id" ,"name" ,"fieldApiName" ,"parentSurveyMapping" ,"childSurveyMapping" ,"formVersion" ,"changeLogNumber"])
    orm_dataframe = pd.concat([orm_dataframe, get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,orm_endpoint, auth_header)])
    # Replace IDs - Replace internal salesforce IDs + actual externalIds with computed external IDs
    form_dataframe_id_replaced = form_dataframe.copy()
    # default to english if form doesn't already contain it
    # assume that only 1 form is present, so this should only be 1 row
    external_id_in_salesforce = form_dataframe_id_replaced['externalId'][0]
    if (not external_id_in_salesforce):
        external_id_in_salesforce = form_dataframe_id_replaced['name'][0]
    taro_id_full = external_id_in_salesforce
    taro_id_parent_form = external_id_in_salesforce
    
    #remove taroId column, just use name for this
    #form_dataframe_id_replaced['changeLog'] = changelog_number
    form_dataframe_id_replaced = form_dataframe_id_replaced.drop(columns=['id','externalId','formVersion'])
    questions_without_options_id_replaced = questions_without_options.copy()
    #Replace relevant columns with "::" suffix if multi-language is possible
    
    questions_id_lookup = questions_without_options_id_replaced[['id','name']].rename(columns={'name':'questionName','id':'questionId'})
    questions_without_options_id_replaced = questions_without_options_id_replaced.merge(questions_id_lookup,how="left",left_on="parent",right_on="questionId").rename(columns={'questionName':'parentName'})
    #remove taroId column, just use name for this
    #questions_without_options_id_replaced['taroId'] = questions_without_options_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else x['name'], axis=1)
    questions_without_options_id_replaced.drop(columns=['externalId'],inplace=True)
    questions_without_options_id_replaced = questions_without_options_id_replaced.drop(columns=['id','changeLogNumber','form','formVersion','questionId','parent'])
    options_dataframe_id_replaced = options_dataframe.copy()
    
    options_dataframe_id_replaced = options_dataframe_id_replaced.merge(questions_id_lookup,how='left',on='questionId')
    #remove taroId column, just use name for this
    # if (not options_dataframe_id_replaced.empty):
    #     options_dataframe_id_replaced['taroId'] = options_dataframe_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else x['name'], axis=1)
    # else:
    #     options_dataframe_id_replaced['taroId'] = None
    options_dataframe_id_replaced.drop(columns=['externalId'],inplace=True)
    options_dataframe_id_replaced = options_dataframe_id_replaced.drop(columns=['id','questionId'])
    field_mapping_without_questions_id_replaced = field_mapping_without_questions.copy()
    field_mapping_without_questions_id_replaced = field_mapping_without_questions_id_replaced.merge(questions_id_lookup,how="left",left_on="repeat",right_on="questionId")
    field_mapping_id_lookup = field_mapping_without_questions_id_replaced[['id','name']].rename(columns={'id':'fieldMappingId','name':'fieldMappingName'})
    #remove taroId column, just use name for this
    # if (not field_mapping_without_questions_id_replaced.empty):
    #     field_mapping_without_questions_id_replaced['taroId'] = field_mapping_without_questions_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else x['name'], axis=1)
    # else:
    #     field_mapping_without_questions_id_replaced['taroId'] = None
    field_mapping_without_questions_id_replaced.drop(columns=['externalId'],inplace=True)
    field_mapping_without_questions_id_replaced = field_mapping_without_questions_id_replaced.drop(columns=['id','form','formVersion','changeLogNumber','repeat','questionId'])
    field_mapping_without_questions_id_replaced = field_mapping_without_questions_id_replaced.rename(columns={'questionName':'repeatQuestionName'}).fillna('')
    question_mapping_dataframe_id_replaced = question_mapping_dataframe.copy()
    question_mapping_dataframe_id_replaced = question_mapping_dataframe_id_replaced.merge(field_mapping_id_lookup,how="left",left_on="field_mapping_id",right_on="fieldMappingId")
    question_mapping_dataframe_id_replaced = question_mapping_dataframe_id_replaced.merge(questions_id_lookup,left_on='question',right_on = 'questionId')
    #remove taroId column, just use name for this
    # if (not question_mapping_dataframe_id_replaced.empty):
    #     question_mapping_dataframe_id_replaced['taroId'] = question_mapping_dataframe_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else x['name'], axis=1)
    # else:
    #     question_mapping_dataframe_id_replaced['taroId'] = None
    question_mapping_dataframe_id_replaced.drop(columns=['externalId'],inplace=True)
    question_mapping_dataframe_id_replaced = question_mapping_dataframe_id_replaced.drop(columns=['id','question','fieldMappingId','field_mapping_id','questionId'])
    skip_logic_dataframe_id_replaced = skip_logic_dataframe.copy()
    skip_logic_dataframe_id_replaced = skip_logic_dataframe_id_replaced.merge(questions_id_lookup,left_on='sourceQuestion',right_on='questionId').rename(columns={'questionName':'sourceQuestionName'}).drop(columns=['questionId'])

    skip_logic_dataframe_id_replaced = skip_logic_dataframe_id_replaced.merge(questions_id_lookup,left_on='parentQuestion',right_on='questionId').rename(columns={'questionName':'parentQuestionName'}).drop(columns=['questionId'])
    #create a fictitious name column (external ID if it exists, join column if not)
    if (not skip_logic_dataframe_id_replaced.empty):
        skip_logic_dataframe_id_replaced['name'] = skip_logic_dataframe_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else str(x['sourceQuestion']) + str(x['parentQuestion']), axis=1)
    else:
        skip_logic_dataframe_id_replaced['name'] = None
    skip_logic_dataframe_id_replaced.drop(columns=['externalId'],inplace=True)
    #skip_logic_dataframe_id_replaced = skip_logic_dataframe_id_replaced.rename(columns = {'externalId':'taroId'})
    skip_logic_dataframe_id_replaced = skip_logic_dataframe_id_replaced.drop(columns=['id','parentQuestion','sourceQuestion','form','formVersion','changeLogNumber'])
    orm_dataframe_id_replaced = orm_dataframe.copy()
    orm_dataframe_id_replaced = orm_dataframe_id_replaced.merge(field_mapping_id_lookup.rename(columns={'fieldMappingName':'parentSurveyName'}),how='left',left_on='parentSurveyMapping',right_on='fieldMappingId').drop(columns=['fieldMappingId'])
    orm_dataframe_id_replaced = orm_dataframe_id_replaced.merge(field_mapping_id_lookup.rename(columns={'fieldMappingName':'childSurveyName'}),how='left',left_on='childSurveyMapping',right_on='fieldMappingId').drop(columns=['fieldMappingId'])
    #remove taroId column, just use name for this
    # if (not orm_dataframe_id_replaced.empty):
    #     orm_dataframe_id_replaced['taroId'] = orm_dataframe_id_replaced.apply(lambda x: str(x['externalId']) if x['externalId'] else x['name'], axis=1)
    # else: 
    #   orm_dataframe_id_replaced['taroId'] = None
    orm_dataframe_id_replaced.drop(columns=['externalId'],inplace=True)
    orm_dataframe_id_replaced = orm_dataframe_id_replaced.drop(columns=['id','parentSurveyMapping','childSurveyMapping','formVersion','changeLogNumber'])

    #Replace "::" suffixes
    taro_language = 'en'
    if ('::' in external_id_in_salesforce):
        taro_id_parent_form = external_id_in_salesforce.split('::')[0]
        taro_language = external_id_in_salesforce.split('::')[1]

    print(taro_id_parent_form)
    print(taro_language) 
    form_dataframe_id_replaced.rename(columns={'name':'name::'+taro_language,'alias':'alias::'+taro_language,'messageAfterSubmission':'messageAfterSubmission::'+taro_language,'description':'description::'+taro_language}, inplace=True)
    questions_without_options_id_replaced.rename(columns={'caption':'caption::'+taro_language,'dynamicOperation':'dynamicOperation::'+taro_language,'dynamicOperationTestData':'dynamicOperationTestData::'+taro_language,'exampleOfValidResponse':'exampleOfValidResponse::'+taro_language,'responseValidation':'responseValidation::'+taro_language,'hint':'hint::'+taro_language},inplace=True)
    options_dataframe_id_replaced = options_dataframe_id_replaced.rename(columns={'caption':'caption::'+taro_language},inplace=True)
    skip_logic_dataframe_id_replaced = skip_logic_dataframe_id_replaced.rename(columns={'skipValue':'skipValue::'+taro_language},inplace=True)
    # Write an excel sheet
    form_name_to_write = form_name_to_download.replace("/","_").replace("\\","_") + ".xlsx"
    writer = pd.ExcelWriter(workingDirectory + "/" + form_name_to_write,engine='xlsxwriter')
    workbook=writer.book

    # https://datascience.stackexchange.com/questions/46437/how-to-write-multiple-data-frames-in-an-excel-sheet
    # form_dataframe
    # questions_without_options
    # options_dataframe
    # question_mapping_dataframe
    # field_mapping_without_questions
    # skip_logic_dataframe
    # orm_dataframe

    # Replace double quote character with safe quote in all columns
    form_dataframe_id_replaced.to_excel(writer,sheet_name='Forms',startrow=1 , startcol=0,index=False)
    questions_without_options_id_replaced.to_excel(writer,sheet_name='Questions',startrow=1 , startcol=0,index=False)
    options_dataframe_id_replaced.to_excel(writer,sheet_name='Options',startrow=1 , startcol=0,index=False)
    question_mapping_dataframe_id_replaced.to_excel(writer,sheet_name='Question_Mappings',startrow=1 , startcol=0,index=False)
    field_mapping_without_questions_id_replaced.to_excel(writer,sheet_name='Field_Mappings',startrow=1 , startcol=0,index=False)
    skip_logic_dataframe_id_replaced.to_excel(writer,sheet_name='Skip_Logic',startrow=1 , startcol=0,index=False)
    orm_dataframe_id_replaced.to_excel(writer,sheet_name='Object_Relationship_Mappings',startrow=1 , startcol=0,index=False)
    writer.close()
    return persistent_full_question_dataframe

    """# Get All Forms in an Org"""
def get_all_forms_in_org(url_to_query,salesforce_service_url,auth_header,workingDirectory):
    all_forms_endpoint = salesforce_service_url + "formdata/v1?objectType=GetFormData&offset=0&limit=100"
    all_form_dataframe = get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,all_forms_endpoint, auth_header)

    sorted_forms_df = all_form_dataframe.sort_values(by='id',ascending=False)
    persistent_full_question_dataframe = None
    for index, frame in sorted_forms_df.iterrows():
        thisFormName = frame['name']
        print(thisFormName)
        
        persistent_full_question_dataframe = get_all_dataframes_and_write_to_excel_from_form_name(url_to_query,salesforce_service_url,auth_header,workingDirectory,thisFormName, persistent_full_question_dataframe)


