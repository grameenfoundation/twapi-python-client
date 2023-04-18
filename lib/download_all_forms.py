import sys
from download_utilities import get_all_forms_in_org
print(sys.argv[1]) # prints var1
print(sys.argv[2]) # prints var2
print(sys.argv[3]) # prints var2

url_to_query = sys.argv[1]
salesforce_service_url = sys.argv[2]
auth_header = sys.argv[3]

get_all_forms_in_org(url_to_query,salesforce_service_url,auth_header)

# Call With
# > python3 download_all_forms.py org_url auth_header

# For Example:

#/services/apexrest/twapi/