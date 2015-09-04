import click
from multiprocessing import Process
import os
import swiftclient
import hashlib
import json


@click.command()
@click.option('--filename', help='File to be uploaded.', required=True)
@click.option('--segment_size', default=1,
              help='Size of segments the file will be divided into in '
              'megabytes. Default and minimum is 1MB')
@click.option('--container', help='Container to place the file.',
              required=True)
@click.option('--auth_token', help='Swift auth token from swift stat.',
              required=True)
@click.option('--storage_url', help='Storage url found from swift stat -v.',
              required=True)
def init(filename, segment_size, container, auth_token, storage_url):
    """Given the swift credentials, upload the targeted file onto swift as a
    Static Large Object"""

    # Validate auth info
    try:
        swiftclient.client.head_container(storage_url, auth_token, container)
    except:
        click.echo("Invalid authentication information. Check that your"
                   " storage_url is correct or do a swift stat to get a new"
                   " auth token")
        return

    # Counter for segments created.
    segment_counter = 1

    # Variables required by several functions wrapped in a dictionary for
    # convenience.
    args = {
        "filename": filename,
        "segment_size": segment_size,
        "container": container,
        "auth_token": auth_token,
        "storage_url": storage_url
    }

    manifest = []

    initial_file_count = len([name for name in os.listdir('.')])

    with open(filename, "rt") as f:
        while True:

            # We do not want to create more faster too much faster than we can
            # upload and delete them. Keep looping until the number of
            # segments yet to be processed is less than 10
            outstanding_segments = len(
                [name for name in os.listdir('.')]) - initial_file_count
            if outstanding_segments > 10:
                continue

            buf = f.read(int(segment_size * 1048576))
            if not buf:
                 # we've read the entire file in, so we're done.
                break

            segment_name = "{}".format(
                "%08d" % segment_counter
            )

            # Create file
            segment = open(segment_name, "wt")
            segment.write(buf)
            segment.close()

            # The location segments will be store don swift is within a pseudo
            # folder.
            swift_destination = os.path.join(args["filename"] + "_segments",
                                             segment_name)

            # Create manifest entry
            manifest.append(create_manifest_entry(segment_name,
                            os.path.join(container, swift_destination)))

            # Upload and delete the segment.
            p = Process(target=process_segment,
                        args=(args, segment_name, swift_destination))
            p.start()

            # Update manifest
            segment_counter += 1

    # Create manifest file
    with open('tempmanifest.json', 'w') as outfile:
        json.dump(manifest, outfile)
        outfile.close()

    # Upload manifest file
    with open('tempmanifest.json', 'r') as outfile:
        swiftclient.client.put_object(storage_url, auth_token, container,
                                      filename, outfile,
                                      query_string="multipart-manifest=put")
        outfile.close()

    delete_file('tempmanifest.json')


def process_segment(args, segment_name, swift_destination):
    '''Given a segment_name, upload the segment and delete the file.'''

    upload_segment(segment_name, swift_destination, args)

    delete_file(segment_name)


def upload_segment(source, target, args):
    '''Upload source to swift at the given taret.'''

    opened_source_file = open(source, 'r')
    swiftclient.client.put_object(args["storage_url"], args["auth_token"],
                                  args["container"], target,
                                  opened_source_file)


def delete_file(filename):
    '''Delete the given file.'''
    os.remove(filename)


def create_manifest_entry(segment_name, swift_destination):
    '''Create a dictionary with the necessary manifest
    variables for the given segment.'''

    size = os.stat(segment_name).st_size
    etag = md5Checksum(segment_name)

    return {
        "path": swift_destination,
        "etag": etag,
        "size_bytes": size
    }


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
    init()
