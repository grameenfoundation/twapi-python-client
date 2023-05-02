import sys
from lib.download_utilities import  get_all_dataframes_and_write_to_excel_from_form_name
print(sys.argv[1]) 
print(sys.argv[2]) 
print(sys.argv[3]) 

url_to_query = sys.argv[1]
salesforce_service_url = sys.argv[2]
auth_header = "OAuth " + sys.argv[3]
workingDirectory = sys.argv[4]
form_name_to_download = sys.argv[5]

get_all_dataframes_and_write_to_excel_from_form_name(url_to_query,salesforce_service_url,auth_header,workingDirectory,form_name_to_download)

# Call With
# > python3 download_one_form.py org_url salesforce_url auth_header workingDir form_name

# For Example:

# python3 download_all_forms.py "ability-energy-2891-dev-ed.scratch.my.salesforce.com" "/services/apexrest/" "00D010000008iR3\!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4" "tmp/"
# or
# debug config
#   "args" : ["ability-energy-2891-dev-ed.scratch.my.salesforce.com", "/services/apexrest/", "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4", "tmp/"]

# The salesforce url will depend on whether you're using managed or unmanaged code:
#/services/apexrest/twapi/
#/services/apexrest/