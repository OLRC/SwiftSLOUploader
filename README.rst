===============================
Swift SLO Uploader
===============================


Swift SLO Uploader was created to upload really large files to Swift quickly using the SLO middleware (Static Large Object).

*******************
Requirements
*******************

* Python 2.7+
* Python Click
* Python SwiftClient

*******************
Installing
*******************

For now, git clone::

    git clone https://github.com/OLRC/SwiftSLOUploader.git

*******************
Basic Usage
*******************

1. Run the following command in the repo after downloading the repo ::

    $ python slo_upload.py --filename path/to/file --container yourcontainer --auth_token yourauthtoken --storage_url https://olrc.scholarsportal.info:8080/v1/AUTH_yourstorageurl

Run the following to get your storage_url and auth_token ::

	$ swift stat -v

This script creates 1MB segments of your file by default. You can increase the segment size with the --segment_size flag. However this  requires a minimum of 10 X segment_size MB of storage space. The script creates segments and deletes them once they're uploaded but maintains a maximum of 10 segments at a time.

This script also creates a container called "--container"_segments to store segments while the actual file will be accessible from the specified --container.


*******************
Options
*******************

::

	$ python slo_upload.py --help
 	$ --filename TEXT         File to be uploaded.  [required]
 	$ --segment_size INTEGER  Size of segments the file will be divided into in
 	$                         megabytes. Default and minimum is 1MB
 	$ --container TEXT        Container to place the file.  [required]
 	$ --auth_token TEXT       Swift auth token from swift stat.  [required]
 	$ --storage_url TEXT      Storage url found from swift stat -v.  [required]
 	$ --concurrent_processes  Number of concurrent processes used to upload segments. Default is 10
 	$ --max_disk_space        In MB, the max amount of disk space the script can use while creating segments.
 	$ --help                  Show this message and exit.



