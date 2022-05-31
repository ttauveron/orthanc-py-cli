#!/usr/bin/env python


import bz2
import gzip
import json
import os
import sys
import tarfile
import zipfile

from beren import Orthanc

IMPORTED_STUDIES = set()
COUNT_ERROR = 0
COUNT_DICOM = 0
COUNT_JSON = 0

ignore_errors = True
verbose = True


class OrthancUpload:
    def __init__(self, orthanc: Orthanc):
        self._orthanc = orthanc

    def __is_json(self, content):
        try:
            if sys.version_info >= (3, 0):
                json.loads(content.decode())
                return True
            else:
                json.loads(content)
                return True
        except:
            return False

    def __upload_buffer(self, dicom):
        global IMPORTED_STUDIES
        global COUNT_ERROR
        global COUNT_DICOM
        global COUNT_JSON

        if self.__is_json(dicom):
            COUNT_JSON += 1
            return

        info = None
        try:
            info = self._orthanc.add_instance(dicom=dicom)
        except:
            COUNT_ERROR += 1
            if ignore_errors:
                if verbose:
                    print('  not a valid DICOM file, ignoring it')
                return
            else:
                raise

        COUNT_DICOM += 1

        if not info['ParentStudy'] in IMPORTED_STUDIES:
            IMPORTED_STUDIES.add(info['ParentStudy'])

            tags = self._orthanc.get_instance_tags(info['ID'], short=True)

            print('')
            print('New imported study:')
            print('  Orthanc ID of the patient: %s' % info['ParentPatient'])
            print('  Orthanc ID of the study: %s' % info['ParentStudy'])
            print('  DICOM Patient ID: %s' % (
                tags['0010,0020'] if '0010,0020' in tags else '(empty)'))
            print('  DICOM Study Instance UID: %s' % (
                tags['0020,000d'] if '0020,000d' in tags else '(empty)'))
            print('')

    def __upload_file(self, path):
        with open(path, 'rb') as f:
            dicom = f.read()
            if verbose:
                print('Uploading: %s (%dMB)' % (path, len(dicom) / (1024 * 1024)))

            self.__upload_buffer(dicom)

    def __upload_bzip2(self, path):
        with bz2.BZ2File(path, 'rb') as f:
            dicom = f.read()
            if verbose:
                print('Uploading: %s (%dMB)' % (path, len(dicom) / (1024 * 1024)))

            self.__upload_buffer(dicom)

    def __upload_gzip(self, path):
        with gzip.open(path, 'rb') as f:
            dicom = f.read()
            if verbose:
                print('Uploading: %s (%dMB)' % (path, len(dicom) / (1024 * 1024)))

            self.__upload_buffer(dicom)

    def __upload_tar(self, path, decoder):
        if verbose:
            print('Uncompressing tar archive: %s' % path)
        with tarfile.open(path, decoder) as tar:
            for item in tar:
                if item.isreg():
                    f = tar.extractfile(item)
                    dicom = f.read()
                    f.close()

                    if verbose:
                        print('Uploading: %s (%dMB)' % (item.name, len(dicom) / (1024 * 1024)))

                    self.__upload_buffer(dicom)

    def __upload_zip(self, path):
        if verbose:
            print('Uncompressing ZIP archive: %s' % path)
        with zipfile.ZipFile(path, 'r') as zip:
            for item in zip.infolist():
                # WARNING - "item.is_dir()" would be better, but is not available in Python 2.7
                if item.file_size > 0:
                    dicom = zip.read(item.filename)
                    if verbose:
                        print('Uploading: %s (%dMB)' % (item.filename, len(dicom) / (1024 * 1024)))
                    self.__upload_buffer(dicom)

    def __decode_file(self, path):
        extension = os.path.splitext(path)[1]

        if path.endswith('.tar.bz2'):
            self.__upload_tar(path, 'r:bz2')
        elif path.endswith('.tar.gz'):
            self.__upload_tar(path, 'r:gz')
        elif extension == '.zip':
            self.__upload_zip(path)
        elif extension == '.tar':
            self.__upload_tar(path, 'r')
        elif extension == '.bz2':
            self.__upload_bzip2(path)
        elif extension == '.gz':
            self.__upload_gzip(path)
        else:
            self.__upload_file(path)

    def upload(self, files):
        for path in files:
            if os.path.isfile(path):
                self.__decode_file(path)
            elif os.path.isdir(path):
                for root, dirs, files in os.walk(path):
                    for name in files:
                        self.__decode_file(os.path.join(root, name))
            else:
                raise Exception('Missing file or directory: %s' % path)

        print('')

        if COUNT_ERROR == 0:
            print('SUCCESS:')
        else:
            print('WARNING:')

        print('  %d DICOM instances properly imported' % COUNT_DICOM)
        print('  %d DICOM studies properly imported' % len(IMPORTED_STUDIES))
        print('  %d JSON files ignored' % COUNT_JSON)
        print('  Error in %d files' % COUNT_ERROR)
        print('')
