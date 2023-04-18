import http.client
import json
from types import SimpleNamespace
import pandas as pd
from urllib.parse import quote

def get_oauth_token_from_login(url_to_query, client_id, client_secret, username, password):
    conn = http.client.HTTPSConnection(url_to_query)
    payload = ''
    headers = {}
    #NOTE: This is bad security practice, normally we would want these login details to be hidden so they don't get backed up to git
    #For simplicity of this demo, we're exposing details here
    #TODO: before production usage, separate these variables into a JSON file that's loaded from Google Drive
    login_url = "/services/oauth2/token?grant_type=password&client_id=" + client_id + "&client_secret=" + client_secret + "&username=" + username + "&password=" + password
    conn.request("POST",  login_url, payload, headers)
    res = conn.getresponse()
    data = res.read()
    decoded_form_data = data.decode("utf-8")
    data_obj = json.loads(decoded_form_data, object_hook=lambda d: SimpleNamespace(**d))
    print(data_obj)
    return "OAuth " + data_obj.access_token

def get_version_changelog_from_form_name(url_to_query,salesforce_service_url,auth_header,form_name):
    form_name_urlsafe = quote(form_name, safe='/')
    form_endpoint = salesforce_service_url + "formdata/v1?objectType=GetFormData&name=" + form_name_urlsafe
    form_dataframe = get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url,form_endpoint,auth_header)
    try:
      form_id = form_dataframe.id[0]
      form_external_id = form_dataframe.externalId[0]
    except: 
      print('No form matches that name')
      return '', '', '','', None
    # Note - this script assumes there is only 1 form matching the name
    json_form_version = form_dataframe.formVersion[0]
    form_version_string = str(json_form_version[0]).replace('\'','"')
    form_version_json_obj = json.loads(form_version_string)
    form_version_id = form_version_json_obj['versionid']
    changelog_number = form_version_json_obj['changeLogNumber']
    print('Form Version ID: ',form_version_id, ' Form ID: ', form_id, ' Changelog: ', changelog_number, ' externalID: ',form_external_id)
    return form_version_id, changelog_number, form_id, form_external_id, form_dataframe

def get_pandas_dataframe_from_json_web_call(url_to_query,salesforce_service_url, endpoint_to_hit, auth_header):
    conn = http.client.HTTPSConnection(url_to_query)
    payload = ''
    headers = {
      'Authorization': auth_header,
      'Content-Type': 'application/json',
    }
    conn.request("GET",  endpoint_to_hit, payload, headers)
    res = conn.getresponse()
    data = res.read()
    decoded_form_data = data.decode("utf-8")
    data_obj = json.loads(decoded_form_data)
    records_dataframe = pd.json_normalize(data_obj, record_path =['records'])
    return records_dataframe

def upload_payload_to_url(url_to_query,salesforce_service_url, auth_header, endpoint_to_upload, payload):
      conn = http.client.HTTPSConnection(url_to_query)
      headers = {
        'Authorization': auth_header,
        'Content-Type': 'application/json',
      }
      conn.request("PUT", endpoint_to_upload, payload.encode(), headers)
      res = conn.getresponse()
      data = res.read()
      decoded_form_data = data.decode("utf-8")
      data_obj = json.loads(decoded_form_data)
      results_dataframe = pd.json_normalize(data_obj)
      print(data.decode("utf-8"))
      return results_dataframe
