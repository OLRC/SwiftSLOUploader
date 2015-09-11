import click
from multiprocessing import Process, Lock
import os
import swiftclient
import hashlib
import json
import time
import math

@click.command()
@click.option('--filename', help='File to be uploaded.', required=True)
@click.option('--segment_size', default=1,
              help='Size of segments the file will be divided into in '
              'megabytes. Default and minimum is 1MB')
@click.option('--container', help='Container to place the file.',
              required=True)
@click.option('--auth_token', help='Swift auth token from swift stat.')
@click.option('--storage_url', help='Storage url found from swift stat -v.')
@click.option('--concurrent_processes', default=10,
              help='Number of concurrent processes used to upload segments.'
              ' Default is 10')
def slo_upload(filename, segment_size, container, auth_token, storage_url,
               concurrent_processes):
    """Given the swift credentials, upload the targeted file onto swift as a
    Static Large Object"""

    # Check credentials
    (auth_token, storage_url) = validate_credentials(storage_url, auth_token,
                                                     container)

    # Check args meet 1000 segment limit.
    (file_size, total_segments, segment_size) = check_segment_size(
        filename, segment_size)

    # Prompt user to proceed with modified arguments
    # segment size
    # processes used
    # space required

    # Variables required by several functions wrapped in a dictionary for
    # convenience.
    args = {
        "filename": filename,
        "segment_size": segment_size,
        "container": container,
        "auth_token": auth_token,
        "storage_url": storage_url,
        "lock": Lock(),
    }

    processes = []  # Holder for processes

    segment_counter = 1  # Counter for segments created.


    with open(filename, "r") as f:

        # Check if upload_cache exists:
        try:
            open("upload_cache")
            segment_counter = fast_forward_file(f, segment_size)
            f.seek(segment_size * 1048576 * (segment_counter - 1))
        except IOError:
            pass
        while True:

            # Stop loop when entire file is read.
            if f.tell() == file_size:
                break

            # Control the maximum number of processes are active.
            # This also restricts how much space is used up for segments.
            while len(processes) >= concurrent_processes:
                p = processes.pop()
                p.join()

            segment_name = "{}".format("%08d" % segment_counter)

            # Create segment
            segment = open(segment_name, "w")

            # We want to read a maximum of 1MB at a time
            read_increment = 1048576
            for i in range(0, int(segment_size * 1048576), read_increment):
                buf = f.read(read_increment)
                segment.write(buf)
            segment.close()

            # The location segments will be stored on swift is within a pseudo
            # folder.
            swift_destination = os.path.join(
                filename.split("/")[-1] + "_segments", segment_name)

            # Upload and delete the segment.
            p = Process(target=process_segment,
                        args=(args, segment_name, swift_destination))
            p.start()
            processes = [p] + processes

            segment_counter += 1
    f.close()

    while len(processes) > 0:
        p = processes.pop()
        p.join()

    # Create manifest file
    create_manifest_file("manifest.json", container)

    # Upload manifest file
    upload_manifest_file("manifest.json", args)

    delete_file("upload_cache")
    delete_file("manifest.json")

def check_segment_size(filename, segment_size):
    '''Check the given file can be segmented within less than or equal to 1000
     segments with the given segment_size. If not, find an appropriate
     segment_size to meet this limit. Return file_size, total_segments and
     segment_size.'''

    file_size = os.stat(filename).st_size
    total_segments = int(math.ceil(
        float(file_size) / float(segment_size * 1048576)))
    if total_segments > 1000:
        click.echo("Unable to use {0} as segment_size due to 1000 segment"
                   " limit.".format(segment_size))
        segment_size = int(math.ceil(float(file_size)/1000.0) / 1048576.0)
        total_segments = int(math.ceil(
            float(file_size) / float(segment_size * 1048576)))

    return (file_size, total_segments, segment_size)



def fast_forward_file(file, segment_size):
    '''Find the last sequential segment name from upload_cache. Fast forward
    the given file to the corresponding position for the next segment. Return
    the next segment number.'''

    segments = []

    cache = open('upload_cache', "r")
    for line in cache:
        segments.append(int(line.split(":")[0]))

    segments.sort()

    i = 1

    while i < len(segments):
        if segments[i - 1] != segments[i] - 1:
            return segments[i - 1] + 1
        i += 1

    return segments[-1] + 1


def validate_credentials(storage_url, auth_token, container):
    '''Validate credentials. Check to make sure auth token and storage url
    work for the given tenant. If they are not set, check that the required
    os variables are set and that the work. Return valid auth token and storage
    url. Exit program if credentials are invalid or os variables not set.'''

    # Check given credentials
    if not (storage_url or auth_token):

        # Check OS variables
        if (not os.environ.get("OS_AUTH_URL") or
                not os.environ.get("OS_USERNAME") or
                not os.environ.get("OS_PASSWORD") or
                not os.environ.get("OS_TENANT_NAME")):

            # Exit if variables are not set.
            click.echo("Please pass in --storage_url and --auth_token or make"
                       " sure $OS_USERNAME, $OS_PASSWORD, $OS_TENANT_NAME,"
                       " $OS_AUTH_URL are set in your environment variables.")
            exit(0)
        else:
            try:
                (storage_url, auth_token) = swiftclient.client.get_auth(
                    os.environ.get("OS_AUTH_URL"),
                    os.environ.get("OS_TENANT_NAME") + ":"
                    + os.environ.get("OS_USERNAME"),
                    os.environ.get("OS_PASSWORD"),
                    auth_version=2)

            except swiftclient.client.ClientException:
                click.echo("Failed to authenticate. Please check that your"
                           " environment variables $OS_USERNAME, $OS_PASSWORD,"
                           " $OS_AUTH_URL and $OS_TENANT_NAME are correct."
                           " Alternatively, pass in --storage_url and"
                           " --auth_token.")
                exit(0)

    # Check credentials against the given container
    try:
        swiftclient.client.head_container(storage_url, auth_token,
                                          container)
    except:
        click.echo("Invalid authentication information. Check that your"
                   " storage_url is correct or do a swift stat to get a new"
                   " auth token")
        exit(0)

    return (auth_token, storage_url)


def process_segment(args, segment_name, swift_destination):
    '''Given a segment_name, upload the segment and delete the file.'''

    upload_segment(segment_name, swift_destination, args)

    log_segment(segment_name, swift_destination, args)

    delete_file(segment_name)

    exit(0)


def upload_segment(source, target, args):
    '''Upload source to swift at the given taret.'''

    opened_source_file = open(source, 'r')
    swiftclient.client.put_object(args["storage_url"], args["auth_token"],
                                  args["container"], target,
                                  opened_source_file)


def log_segment(segment_name, swift_destination, args):
    '''Write to the upload_cache the segment that was uploaded with it's
    swift_destination'''

    args["lock"].acquire()
    open('upload_cache', 'a').write(
        "{0}:{1}:{2}:{3}\n".format(
            segment_name, swift_destination, md5Checksum(segment_name),
            os.stat(segment_name).st_size))
    args["lock"].release()


def create_manifest_file(filename, container):
    '''From the upload cache, create the manifest file.'''

    manifest = []

    # Create a manifest file for writing.
    with open(filename, 'w') as outfile:

        # Read lines from upload_cache
        cache = open('upload_cache', "r")
        for line in cache:
            manifest.append(create_manifest_entry(line, container))

        manifest = sorted(manifest, key=lambda k: k['name'])

        # We sort by name but the manifest json entries do not have name in
        # them.
        for entry in manifest:
            del entry["name"]
        json.dump(manifest, outfile)


def create_manifest_entry(line, container):
    '''Create and return a dictionary with the necessary manifest
    variables for the given segment.'''

    parts = line.split(":")

    return {
        "name": parts[0],
        "path": os.path.join(container, parts[1]),
        "etag": parts[2],
        "size_bytes": parts[3]
    }


def upload_manifest_file(manifest_name, args):
    '''Open the given file and upload it. If the upload fails, attempt to
    reupload it up to 9 times with exponential waits in between attempts.'''

    with open(manifest_name, 'r') as outfile:

        # Filename is the local path to the file. The manifest needs to be
        # the name of the file.
        filename = args["filename"].split("/")[-1]

        try:
            swiftclient.client.put_object(
                args["storage_url"], args["auth_token"], args["container"],
                filename, outfile,
                query_string="multipart-manifest=put")
            click.echo(
                "Upload successful!")
        except Exception, e:
            print(e)
            click.echo(
                "Upload failed. Manifest could not be uploaded.")
    outfile.close()


def delete_file(filename):
    '''Delete the given file.'''
    os.remove(filename)


def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()


if __name__ == '__main__':
    slo_upload()
