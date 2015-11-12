===============================
Swift SLO Uploader
===============================


Swift SLO Uploader was created to upload large files to `Swift <http://docs.openstack.org/developer/swift/>`_ quickly using the `SLO middleware (Static Large Object). <http://docs.openstack.org/developer/swift/middleware.html#slo-doc>`_ This is achieved by creating segments of the original file which are then uploaded concurrently. A manifest file, which is a mapping of all the segments, is uploaded once all the segments are created and used by Swift when users download the entire file in the future.

*******************
Requirements
*******************

* Python 2.7+
* `Python Click <http://click.pocoo.org/5/>`_
* `Python SwiftClient <https://github.com/openstack/python-swiftclient>`_

*******************
Installing
*******************

 ::

    git clone https://github.com/OLRC/SwiftSLOUploader.git
    cd SwiftSLOUploader
    sudo python setup.py install

*******************
Basic Usage
*******************
1. Set the following environment variables::

	$ export OS_USERNAME=yourusername
	$ export OS_PASSWORD=yourpassword
	$ export OS_TENANT_NAME=yourtenantname
	$ export OS_AUTH_URL=yourcluserauthurl

Alternatively, if you have an auth token and the storage url you can instead use the auth-token and storage-url options described below.

2. Run the following command ::

    $ swiftslouploader path/to/file yourcontainer


Using options ::

	$ swiftslouploader path/to/file yourcontainer --segment-size 5
	$ swiftslouploader path/to/file yourcontainer --concurrent-processes 10

**************
Usage Notes
**************

This script creates 1MB segments of your file by default. You can increase the segment size with the segment-size option. However this requires a minimum of segment-size MB of storage space. The script creates segments and deletes them once they're uploaded but maintains a maximum number of segments at a time as set by the concurrent-processes option (default is 1).

This script also creates a container called "<container>_segments", where <container> is the specified container to store the file. This segments container is where the objects segments will be stored. The object will be accessible from the specified container.

Swiftslouploader will create a directory called 'temp' in the current working directory. It will store segments and relevant files in it during the upload process and will delete the directory upon successful upload. **Warning:** if your current working directory has a temp folder, it will be deleted. Use the temp-directory option to specify another location for the temp directory to be created.

*******************
Options
*******************

segment-size
------------

Size of segments the file will be divided into in MB. The default and minimum is 1MB.

**note:** Swift SLOs have a maximum number of segments of 1000. Due to this restriction, swiftslouploader will recalculate a larger segment size if required.

auth-token
----------

In lieu of setting environment variables, an auth token along with the storage-url option can be passed in instead.

storage-url
-----------

In lieu of setting environment variables, a storage url along with the auth-token option can be passed in instead.

concurrent-processes
--------------------

In order to speed up the creation and uploading of segments, by default swiftslouploader creates 1 process that run concurrently. Use this option to set the number of concurrent processes used.

**note:** Increasing the number concurrent processes increases the amount of disk space swiftslouploader uses. If more disk space is required than is set by the max-disk-space option, the number of concurrent processes is recalculated to not exceed the maximum.

max-disk-space
--------------

By default, swiftslouploader uses (segment-size x concurrent-processes) in MB of disk space when creating segments. This can be restricted by passing in an int value in MB.

temp-directory
--------------

Swiftslouploader uses the current working directory to create a directory called temp. This directory is used to create segments and relevant files and is then deleted. With this option, you can specify where this temporary directory is created.

**note:** If the directory passed in already contains a "temp" directory, it will be deleted.

summary
-------
 ::

	$ python slo_upload.py --help
 	$ --segment-size INTEGER  Size of segments the file will be divided into in
 	$                         megabytes. Default and minimum is 1MB
 	$ --auth-token TEXT       Swift auth token from swift stat.
 	$ --storage-url TEXT      Storage url found from swift stat -v.
 	$ --concurrent-processes  Number of concurrent processes used to upload segments. Default is 1
 	$ --max-disk-space        In MB, the max amount of disk space the script can use while creating segments. By default, the script will use as much space as required as determined by the segment_size and concurrent_processes
 	$ --temp-directory        The directory used temporarily for the creation of segments. By default, a directory named temp is created. Warning: this directory will be deleted.
 	$ --help                  Show this message and exit.
