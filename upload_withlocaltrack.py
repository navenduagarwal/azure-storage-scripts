import argparse
import json
import logging
import os

from azure.storage.blob import BlockBlobService

logger = logging.getLogger('Azure-Upload')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-f", "--folder", required=True,
                help="path to root folder name", default='D:/testing/')
ap.add_argument("-a", "--accountname", required=False,
                help="azure account name", default='modeldocuments')
ap.add_argument("-k", "--key", required=True,
                help="azure account key")
args = vars(ap.parse_args())

rootdir = args["folder"]
if rootdir[-1] != '/':
    rootdir = rootdir + '/'

listFile = 'uploaded.txt'


def run():
    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name=args["accountname"], account_key=args["key"])

        for subdir, dirs, files in os.walk(rootdir):
            filename = subdir[subdir.rfind('/') + 1:]
            number_files = len(files)
            print("\nSub Folder Name: {}, Total Files {}".format(filename, number_files))
            container_name = filename.lower()  # valid names only lower case alphanumeric plus dash
            # Create a container called filename.
            if number_files > 0:
                block_blob_service.create_container(container_name)
                for count, local_file_name in enumerate(files):
                    try:
                        # logger.info("File {}".format(local_file_name))
                        full_path_to_file = os.path.join(subdir, local_file_name)
                        # logger.info("Full path {}".format(full_path_to_file))
                        uploaded_tag = False
                        with open(listFile, 'r') as filehandle:
                            basicList = json.load(filehandle)
                            # print("Existing- {}".format(len(basicList)))
                            if local_file_name not in basicList:
                                uploaded_tag = True
                                block_blob_service.create_blob_from_path(container_name, local_file_name,
                                                                         full_path_to_file)
                                logger.info("uploaded {}/{}".format(count + 1, number_files))
                            else:
                                logger.warning("File Already Exist, skipping -  {}".format(local_file_name))
                        if uploaded_tag:
                            logger.info("adding file")
                            with open(listFile, 'r') as filehandle:
                                basicList = json.load(filehandle)
                                basicList.extend([local_file_name])
                                with open(listFile, 'w') as filehandle:
                                    json.dump(basicList, filehandle)
                    except Exception as e:
                        logger.error("Error occurred during creation loop with file {}".format(local_file_name))
                        print(e)
    except Exception as e:
        print(e)


# Main method.
if __name__ == '__main__':
    run()
