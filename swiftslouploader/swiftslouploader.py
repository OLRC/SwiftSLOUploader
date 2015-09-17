import click
from multiprocessing import Process, Lock
import os
import swiftclient
import hashlib
import json
import time
import math
import shutil


@click.command()
@click.argument('filename')
@click.argument('container')
@click.option('--segment_size', default=1,
              help='Size of segments the file will be divided into in '
              'megabytes. Default and minimum is 1MB')
@click.option('--auth_token', help='Swift auth token from swift stat.')
@click.option('--storage_url', help='Storage url found from swift stat -v.')
@click.option('--concurrent_processes', default=10,
              help='Number of concurrent processes used to upload segments.'
              ' Default is 10')
@click.option('--max_disk_space', default=0,
              help='In MB, the max amount of disk space the script can use'
              ' while creating segments. By default, the script will use as'
              ' much space as required as determined by the segment_size and'
              ' concurrent_processes')
@click.option('--temp_directory',
              help='The directory used temporarily for the creation of'
              ' segments. By default, a directory named temp is created.'
              ' Warning: this directory will be deleted.')
def slo_upload(filename, container, segment_size, auth_token, storage_url,
               concurrent_processes, max_disk_space, temp_directory):
    """Given the swift credentials, upload the targeted file onto swift as a
    Static Large Object"""

    # Check credentials
    (auth_token, storage_url) = validate_credentials(storage_url, auth_token,
                                                     container)

    # Check args meet 1000 segment limit.
    (file_size, total_segments, segment_size) = check_segment_size(
        filename, segment_size)

    # Check args meet max_disk_space
    concurrent_processes = update_concurrent_processes(
        concurrent_processes, segment_size, max_disk_space)

    # Adjust the temp directory
    temp_directory = adjust_temp_directory(temp_directory)

    # Variables required by several functions wrapped in a dictionary for
    # convenience.
    args = {
        "filename": filename,
        "segment_size": segment_size,
        "container": container,
        "auth_token": auth_token,
        "storage_url": storage_url,
        "lock": Lock(),
        "total_segments": total_segments,
        "segment_size": segment_size,
        "file_size": file_size,
        "concurrent_processes": concurrent_processes,
        "processes": [],  # Holder for processes,
        "temp_directory": temp_directory,
    }

    # Check for existing upload_cache
    segment_counter = get_segment_starting_point(args)
    args["segment_counter"] = segment_counter

    # Prompt user to proceed with modified arguments
    get_user_confirmation(args)

    # Create segments container
    args["segment_container"] = create_container(
        args, container + "_segments")

    # Create and upload the segments
    create_segments(args)

    # Wait for remaining uploads to finish.
    join_processes(args["processes"])

    # Create manifest file
    create_manifest_file(
        os.path.join(args["temp_directory"], "manifest.json"), args)

    # Upload manifest file
    upload_manifest_file(
        os.path.join(args["temp_directory"], "manifest.json"), args)

    delete_directory(temp_directory)


def get_segment_starting_point(args):
    '''Return the segment_count where the upload should start based on
    the existence of upload_cache. '''

    try:
        open(os.path.join(args["temp_directory"], "upload_cache"))

        return update_segment_counter(args)

    except IOError:
        return 1


def adjust_temp_directory(temp_directory):
    '''If the temp directory is not set, return the temp directory as temp.
    If the temp directory is set, create return a path to temp within
    temp_directory.'''

    if not temp_directory:
        temp_directory = ''

    return os.path.join(temp_directory, 'temp')


def update_concurrent_processes(concurrent_processes, segment_size,
                                max_disk_space):
    '''Given the number of processes to be run and the
    segment_size, return the number of processes that can run without exceeding
    the max_disk_space.'''

    # Only need to make changes if max_disk_space defined.
    if max_disk_space:
        total_disk_space_used = concurrent_processes * segment_size * 1048576

        if total_disk_space_used > max_disk_space * 1048576:
            click.echo(
                "Unable to use {0} as concurrent_processes due to {1} "
                "max_disk_space limit".format(
                    concurrent_processes, max_disk_space))
            concurrent_processes = max_disk_space / (segment_size)

    # Not possible to stay within minimum disk space
    if not concurrent_processes:
        click.echo(
            "Unable to perform upload with {0}MB max_disk_space. "
            "Minimum disk space required is {1}MB".format(
                max_disk_space, segment_size))
        concurrent_processes = 1
    return concurrent_processes


def create_container(args, container_name):
    '''Create the given container name. Return the name of the container
    created.'''

    swiftclient.client.put_container(args["storage_url"], args["auth_token"],
                                     container_name)
    return container_name


def create_segments(args):
    '''Create segments of the file starting at segment_counter. Create
    processes to upload them, delete them and update the upload_cache file.'''

    segment_counter = args["segment_counter"]  # Counter for segments created.

    # Progress bar for segments uploaded.
    with click.progressbar(length=args["total_segments"],
                           label="Processing segments") as bar:

        # If not creating segments from the beginning, update the progress bar
        if segment_counter > 1:
            bar.update(segment_counter)

        # Check for temp directory, create it if it doens't exist.
        if not os.path.isdir(args["temp_directory"]):
            os.makedirs(args["temp_directory"])

        # Stop loop when all segments are created
        while segment_counter <= args["total_segments"]:

            # Control the maximum number of processes are active.
            # This also restricts how much space is used up for segments.
            while len(args["processes"]) >= args["concurrent_processes"]:
                p = args["processes"].pop()
                p.join()

            segment_name = "{}".format("%04d" % segment_counter)

            # The location segments will be stored on swift is within a
            # pseudo folder.
            swift_destination = os.path.join(
                args["filename"].split("/")[-1] + "_segments",
                segment_name)

            # Craete, upload and delete the segment.
            p = Process(target=process_segment,
                        args=(args, segment_name, swift_destination))
            p.start()
            bar.update(1)
            args["processes"] = [p] + args["processes"]

            segment_counter += 1


def join_processes(processes):
    '''Join all the processes in the list of processes.'''
    click.echo("Wrapping up remaining uploads...")
    while len(processes) > 0:
        p = processes.pop()
        p.join()


def get_filename(args):
    '''Open the cache and return the name of the file.'''

    cache = open(os.path.join(args["temp_directory"], 'upload_cache'), "r")
    for line in cache:
        return line.split(":")[1].rsplit("/", 1)[-2].rsplit("_", 1)[0]


def get_segment_size(args):
    '''Assuming a upload_cache exists, return the segment size as in int in
    MB'''

    cache = open(os.path.join(args["temp_directory"], 'upload_cache'), "r")
    for line in cache:
        return int(line.split(":")[3]) / 1048576


def get_user_confirmation(args):
    '''Prompt user with current variables and prompt the user with confirmation
    to continue. Otherwise exit.'''

    # Confirm continuation of upload
    if args["segment_counter"] > 1:
        click.echo("An incomplete upload has been detected.")

        filename = get_filename(args)

        # If current upload does not match the cache, clear the cache.
        if filename != args["filename"].rsplit("/", 1)[-1]:
            delete_directory(args["temp_directory"])
        else:
            percentage_complete = (
                (float(args["segment_counter"])
                    / float(args["total_segments"])) * 100)

            confirmation = click.prompt(
                "Do you wish to continue upload {0} at {1}% ? (yes/no)".format(
                    filename, percentage_complete))
            while not (confirmation == "no" or confirmation == "yes"):
                confirmation = click.prompt(
                    "Do you wish to continue upload {0}? ({1})".format(
                        filename, percentage_complete))

            if confirmation == "no":
                delete_directory(args["temp_directory"])
                args["segment_counter"] = 1
                click.echo("Clearing cache. Starting new upload.\n")
            else:

                # Confirm segment size is the same as previous upload.
                segment_size = get_segment_size(args)
                if args["segment_size"] != segment_size:
                    click.echo("Continuing upload with former segment_size"
                               " {0}MB.".format(segment_size))
                    args["segment_size"] = segment_size

    click.echo("Please review the following before continuing:")

    # Offset segment_counter by one because segment_counter reflects the
    # segment to create not created.
    click.echo("       segments created: {0}/{1}".format(
        args["segment_counter"] - 1, args["total_segments"]))
    click.echo("          segments size: {0}MB".format(args["segment_size"]))
    click.echo("   concurrent processes: {0}".format(
        args["concurrent_processes"]))
    click.echo("        disk space used: {0}MB".format(
        args["segment_size"] * args["concurrent_processes"]))
    confirmation = click.prompt("Do you wish to proceed? Enter yes or no")
    while not (confirmation == "no" or confirmation == "yes"):
        confirmation = click.prompt("Do you wish to proceed? Enter yes or no")
    if confirmation == "no":
        click.echo("Exiting.")
        exit(0)
    click.echo("Starting up load ...")


def check_segment_size(filename, segment_size):
    '''Check the given file can be segmented within less than or equal to 1000
     segments with the given segment_size. If not, find an appropriate
     segment_size to meet this limit. Return file_size, total_segments and
     segment_size.'''

    file_size = os.stat(filename).st_size
    total_segments = int(math.ceil(
        float(file_size) / float(segment_size * 1048576)))
    if total_segments > 1000:
        click.echo("Unable to use {0}MB as segment_size due to 1000 segment"
                   " limit.".format(segment_size))
        segment_size_bytes = float(math.ceil(file_size/1000.0))
        segment_size = int(math.ceil(segment_size_bytes / 1048576.0))
        total_segments = int(math.ceil(
            float(file_size) / float(segment_size * 1048576)))

    return (file_size, total_segments, segment_size)


def update_segment_counter(args):
    '''Find the last sequential segment name from upload_cache. Return
    the next sequential segment number.'''

    # Gather and sort segments
    segments = []
    cache = open(os.path.join(args["temp_directory"], 'upload_cache'), "r")
    for line in cache:
        segments.append(int(line.split(":")[0]))
    segments.sort()

    if len(segments) == 0:
        return 1

    # Find segment where the next segment is not sequential
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

    # Check credentials
    try:
        swiftclient.client.head_account(storage_url, auth_token)
    except:
        click.echo("Invalid authentication information. Check that your"
                   " storage_url is correct or do a swift stat to get a new"
                   " auth token")
        exit(0)

    # Check credentials against the given container
    try:
        swiftclient.client.head_container(storage_url, auth_token,
                                          container)
    except:
        click.echo("Container does not exist.")
        confirmation = click.prompt(
            "Do you wish to create container {0}? (yes/no)".format(container))
        while not (confirmation == "no" or confirmation == "yes"):
            confirmation = click.prompt(
                "Do you wish to create container {0}?".format(container))
        if confirmation == "yes":
            create_container(
                {"storage_url": storage_url, "auth_token": auth_token},
                container)
        else:
            exit(0)

    return (auth_token, storage_url)


def process_segment(args, segment_name, swift_destination):
    '''Given a segment_name, create the segment, upload it and delete the
    file.'''

    create_segment(args, segment_name)

    upload_segment(
        os.path.join(
            args["temp_directory"], segment_name), swift_destination, args)

    log_segment(segment_name, swift_destination, args)

    delete_file(os.path.join(args["temp_directory"], segment_name))

    exit(0)


def create_segment(args, segment_name):
    '''For the given segment, create the segment based on the numeric value
    of the segment name and segment_size.'''

    with open(args["filename"], "r") as f:

        # Seek file to the corresponding position.
        f.seek(args["segment_size"] * 1048576 * (int(segment_name) - 1))

        # Create segment
        segment = open(
            os.path.join(args["temp_directory"], segment_name), "w")

        # We want to read a maximum of 1MB at a time
        read_increment = 1048576
        for i in range(0, int(args["segment_size"] * 1048576),
                       read_increment):
            buf = f.read(read_increment)
            segment.write(buf)
        segment.close()
    f.close()


def upload_segment(source, target, args):
    '''Upload source to swift at the given taret.'''

    opened_source_file = open(source, 'r')
    swiftclient.client.put_object(args["storage_url"], args["auth_token"],
                                  args["segment_container"], target,
                                  opened_source_file)


def log_segment(segment_name, swift_destination, args):
    '''Write to the upload_cache the segment that was uploaded with it's
    swift_destination'''

    segment_location = os.path.join(args["temp_directory"], segment_name)

    args["lock"].acquire()
    open(os.path.join(args["temp_directory"], 'upload_cache'), 'a').write(
        "{0}:{1}:{2}:{3}\n".format(
            segment_name, swift_destination, md5Checksum(segment_location),
            os.stat(segment_location).st_size))
    args["lock"].release()


def create_manifest_file(filename, args):
    '''From the upload cache, create the manifest file.'''

    manifest = []

    # Create a manifest file for writing.
    with open(filename, 'w') as outfile:

        # Read lines from upload_cache
        cache = open(os.path.join(args["temp_directory"], 'upload_cache'), "r")
        for line in cache:
            manifest.append(
                create_manifest_entry(line, args["segment_container"]))

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


def delete_directory(directory):
    '''Delete the given directory.'''
    shutil.rmtree(directory)


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
