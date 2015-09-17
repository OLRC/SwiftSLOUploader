#!/usr/bin/env python

from setuptools import setup

requirements = [
    "click >= 3.3",
    "python-swiftclient",
]

setup(
    name='SwiftSLOUploader',
    version='0.1.0',
    author='OLRC Collaborators',
    author_email='cloudtech@scholarsportal.info',
    packages=['swiftslouploader'],
    url='https://github.com/OLRC/SwiftSLOUploader',
    license='LICENSE.txt',
    description='Swift SLO Uploader was created to upload really large files'
        ' to Swift quickly using the SLO middleware (Static Large Object).',
    long_description=open('README.rst').read(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'swiftslouploader=swiftslouploader.swiftslouploader:slo_upload',
        ]
    },
)
