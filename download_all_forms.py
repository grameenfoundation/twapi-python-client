import sys
from lib.download_utilities import get_all_forms_in_org
print(sys.argv[1]) # prints var1
print(sys.argv[2]) # prints var2
print(sys.argv[3]) # prints var2

url_to_query = sys.argv[1]
salesforce_service_url = sys.argv[2]
auth_header = "OAuth " + sys.argv[3]

get_all_forms_in_org(url_to_query,salesforce_service_url,auth_header)

# Call With
# > python3 download_all_forms.py org_url auth_header

# For Example:

# python3 download_all_forms.py "ability-energy-2891-dev-ed.scratch.my.salesforce.com" "/services/apexrest/" "00D010000008iR3\!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4"
# or
# debug config
#   "args" : ["ability-energy-2891-dev-ed.scratch.my.salesforce.com", "/services/apexrest/", "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4"]

# The salesforce url will depend on whether you're using managed or unmanaged code:
#/services/apexrest/twapi/
#/services/apexrest/