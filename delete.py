import argparse
import logging
import os

from azure.storage.blob import BlockBlobService

logger = logging.getLogger('Azure-Delete')
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
                try:
                    block_blob_service.delete_container(container_name)
                    logger.info("deleted {}".format(container_name))
                except Exception as e:
                    logger.error(e)
    except Exception as e:
        print(e)


# Main method.
if __name__ == '__main__':
    run()
