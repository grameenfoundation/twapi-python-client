import sys
from lib.unsquish_utilities import *
print(sys.argv[1]) 

sourceDirectory = sys.argv[1]
destDirectory = sys.argv[2]

unsquish_all_files_in_folder(sourceDirectory,destDirectory)


# Call With
# > python3 download_all_forms.py org_url auth_header workingDir

# For Example:

# python3 download_all_forms.py "ability-energy-2891-dev-ed.scratch.my.salesforce.com" "/services/apexrest/" "00D010000008iR3\!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4" "tmp/"
# or
# debug config
#   "args" : ["ability-energy-2891-dev-ed.scratch.my.salesforce.com", "/services/apexrest/", "00D010000008iR3!ARQAQBSTranCGsbhmcj4B2M0xEnJaRXHx_FADdCAyvin.yWRnShWSAIDDbrsBNBXS1B8ZRWsG9ZTv9.Krfs7NAg6G8QvxXm4", "tmp/"]

# The salesforce url will depend on whether you're using managed or unmanaged code:
#/services/apexrest/twapi/
#/services/apexrest/