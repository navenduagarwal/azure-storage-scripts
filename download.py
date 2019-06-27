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
                help="path to root download folder name", default='D:/download/')
ap.add_argument("-a", "--accountname", required=False,
                help="azure account name", default='modeldocuments')
ap.add_argument("-k", "--key", required=True,
                help="azure account key")
args = vars(ap.parse_args())

rootdir = args["folder"]
if rootdir[-1] != '/':
    rootdir = rootdir + '/'

def run():
    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name=args["accountname"], account_key=args["key"])

        containers = block_blob_service.list_containers()
        for c in containers:
            container_name = c.name
            generator = block_blob_service.list_blobs(container_name)
            count_existing_files = (len(generator.items))
            print("\nSub Folder Name: {}, Total Files {}".format(container_name, count_existing_files))
            if count_existing_files > 0:
                full_path_to_container = os.path.join(rootdir, container_name)
                if not os.path.exists(full_path_to_container):
                    os.makedirs(full_path_to_container)
                for count, blob in enumerate(generator):
                    file_name = blob.name
                    full_path_to_file = os.path.join(full_path_to_container, file_name)
                    try:
                        block_blob_service.get_blob_to_path(container_name, file_name, full_path_to_file)
                        logger.info("Downloaded {}/{}".format(count+1,count_existing_files))
                    except:
                        logger.error("Error while downloading {}".format(full_path_to_file))

    except Exception as e:
        print(e)


# Main method.
if __name__ == '__main__':
    run()
