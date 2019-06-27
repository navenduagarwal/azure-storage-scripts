import argparse
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
ap.add_argument("-f", "--folder", required=False,
                help="path to root folder name", default='')
ap.add_argument("-a", "--accountname", required=False,
                help="azure account name", default='modeldocuments')
ap.add_argument("-k", "--key", required=True,
                help="azure account key",
                default='')
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
                block_blob_service.create_container(container_name)
                # List the blobs in the container
                print("Check files existing in the container {}".format(container_name))
                generator = block_blob_service.list_blobs(container_name)
                count_existing_files = (len(generator.items))
                existing_file_names = []
                if count_existing_files > 0:
                    print("Total number of existing files - {}".format(count_existing_files))
                    for blob in generator:
                        existing_file_names.append(blob.name)
                for count, local_file_name in enumerate(files):
                    try:
                        # logger.info("File {}".format(local_file_name))
                        full_path_to_file = os.path.join(subdir, local_file_name)
                        # logger.info("Full path {}".format(full_path_to_file))
                        if local_file_name not in existing_file_names:
                            block_blob_service.create_blob_from_path(container_name, local_file_name,
                                                                     full_path_to_file)
                            logger.info("uploaded {}/{}".format(count + 1, number_files))
                        else:
                            logger.warning("File Already Exist, skipping -  {}".format(local_file_name))
                    except Exception as e:
                        logger.error("Error occurred during creation loop with file {}".format(local_file_name))
                        print(e)
    except Exception as e:
        print(e)


# Main method.
if __name__ == '__main__':
    run()
