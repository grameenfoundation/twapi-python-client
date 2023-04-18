import sys
import os

import lib.upload_utilities

from lib.upload_utilities import upload_all_files_in_folder
print(sys.argv[1]) 
print(sys.argv[2]) 
print(sys.argv[3])
print(sys.argv[4])

url_to_query = sys.argv[1]
salesforce_service_url = sys.argv[2]
auth_header = "OAuth " + sys.argv[3]
workingDirectory = sys.argv[4]

upload_all_files_in_folder(url_to_query,salesforce_service_url,auth_header,workingDirectory)


# Call With
# > python3 upload_all_forms.py org_url auth_header workingDir

# For Example:

# python3 upload_all_forms.py "ability-energy-2891-dev-ed.scratch.my.salesforce.com" "/services/apexrest/" "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4" "tmp/"
# or
# debug config
#   "args" : ["ability-energy-2891-dev-ed.scratch.my.salesforce.com", "/services/apexrest/", "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4", "tmp/"]

# The salesforce url will depend on whether you're using managed or unmanaged code:
#/services/apexrest/twapi/
#/services/apexrest/