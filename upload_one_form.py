import sys
import os

import lib.upload_utilities

from lib.upload_utilities import func_read_excel_file_and_upload
print(sys.argv[1]) 
print(sys.argv[2]) 
print(sys.argv[3])
print(sys.argv[4])

url_to_query = sys.argv[1]
salesforce_service_url = sys.argv[2]
auth_header = "OAuth " + sys.argv[3]
workingDirectory = sys.argv[4]
filename = sys.argv[5]

func_read_excel_file_and_upload(url_to_query,salesforce_service_url,auth_header,workingDirectory, filename)


# Call With
# > python3 upload_all_forms.py org_url auth_header workingDir filename

# For Example:

# python3 upload_all_forms.py "ability-energy-2891-dev-ed.scratch.my.salesforce.com" "/services/apexrest/" "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4" "tmp/" "myfile.xlsx"
# or
# debug config
#   "args" : ["ability-energy-2891-dev-ed.scratch.my.salesforce.com", "/services/apexrest/", "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4", "tmp/", "myfile.xlsx"]

# The salesforce url will depend on whether you're using managed or unmanaged code:
#/services/apexrest/twapi/
#/services/apexrest/