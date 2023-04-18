import sys
from download_utilities import get_all_forms_in_org
print(sys.argv[1]) # prints var1
print(sys.argv[2]) # prints var2

url_to_query = sys.argv[1]
auth_header = sys.argv[2]

get_all_forms_in_org(url_to_query,auth_header)

# Call With
# > python3 download_all_forms.py org_url auth_header

# For Example: