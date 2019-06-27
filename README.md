# Code to upload, download and delete files to Microsoft Azure storage blob
- install requirements files
    - pip install -r requirement.txt
- to upload files
    - python upload -f "folder full path" -a "account name" -k "key"
- to download files
    - python download -f "folder full path" -a "account name" -k "key"
- to delete files
    - python delete -f "folder full path" -a "account name" -k "key"

Key features:
 - automatically create container name for each folder and add files in the same on the Azure storage 
 - don't upload the file again if already existing
 - download files in the same folder structure (level 1)
 