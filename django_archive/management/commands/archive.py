from collections import OrderedDict
from datetime import datetime
from io import BytesIO
from json import dump
from os import path
from tarfile import TarInfo, TarFile

from django.apps.registry import apps
from django.conf import settings
from django.core.files.base import File
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import models
from django.utils.encoding import smart_bytes

from .utils import *

from ... import __version__


class MixedIO(BytesIO):
    """
    A BytesIO that accepts and encodes Unicode data.

    This class was born out of a need for a BytesIO that would accept writes of
    both bytes and Unicode data - allowing identical usage from both Python 2
    and Python 3.
    """

    def rewind(self):
        """
        Seeks to the beginning and returns the size.
        """
        size = self.tell()
        self.seek(0)
        return size

    def write(self, data):
        """
        Writes the provided data, converting Unicode to bytes as needed.
        """
        BytesIO.write(self, smart_bytes(data))


def walk_storage_files(storage, directory=""):
    directories, files = storage.listdir(directory)
    for f in files:
        media_root_relative_path = format(path.join(directory, f))
        storage_file = storage.open(media_root_relative_path , "rb")
        # Some storage (at least FileSystemStorage) do not provide the 'name' argument to the File ctor on opening,
        # which sets File.name to the absolute path. Instead, it should be relative to the media root.
        storage_file.name = media_root_relative_path 
        yield storage_file
    for d in directories:
        for f in walk_storage_files(storage, path.join(directory, d)):
            yield f


class Command(BaseCommand):
    """
    Create an archive of database tables and uploaded media, potentially compressed.
    """

    help = "Create an archive of database tables and uploaded media, potentially compressed."

    def handle(self, *args, **kwargs):
        """
        Process the command.
        """
        
        self.attr = AttributeRepository()

        if not path.isdir(self.attr.get('ARCHIVE_DIRECTORY')):
            self.stderr.write("Setting 'ARCHIVE_DIRECTORY' set to the non-existent directory '{}'."
                              .format(self.attr.get('ARCHIVE_DIRECTORY')))
            exit(1)
            
        with self._create_archive() as tar:
            self._dump_db(tar)
            self._dump_files(tar)
            self._dump_meta(tar)
        self.stdout.write("Backup completed to archive '{}'.".format(tar.name))


    def _create_archive(self):
        """
        Create the archive and return the TarFile.
        """
        filename = self.attr.get('ARCHIVE_FILENAME')
        fmt = self.attr.get('ARCHIVE_FORMAT')
        absolute_path = path.join(
            self.attr.get('ARCHIVE_DIRECTORY'),
            '%s.tar%s' % (datetime.today().strftime(filename), '.'+fmt if fmt else '')
        )
        return TarFile.open(absolute_path, 'w:%s' % fmt)

    def _dump_db(self, tar):
        """
        Dump the rows in each model to the archive.
        """

        # Dump the tables to a MixedIO
        data = MixedIO()
        call_command('dumpdata', all=True, format='json', indent=self.attr.get('ARCHIVE_DB_INDENT'),
                                 exclude=self.attr.get('ARCHIVE_EXCLUDE'), stdout=data)
        info = TarInfo(DB_DUMP)
        info.size = data.rewind()
        tar.addfile(info, data)

    def _dump_files(self, tar):
        if self.attr.get('ARCHIVE_MEDIA_POLICY') == 'all_files':
            self._dump_all_files(tar)
        elif self.attr.get('ARCHIVE_MEDIA_POLICY') == 'filefield_targets':
            self._dump_referenced_files(tar)
        elif self.attr.get('ARCHIVE_MEDIA_POLICY'):
            self.stderr.write("Warning: ARCHIVE_MEDIA_POLICY value '{}' is not supported. Media files not archived."
                              .format(self.attr.get('ARCHIVE_MEDIA_POLICY')))

    def _dump_all_files(self, tar):
        """
        Dump all media files found by the media storage class.
        """

        media_storage = get_mediastorage()
        for file in walk_storage_files(media_storage):
            self._add_file(tar, file)
            file.close()

    def _dump_referenced_files(self, tar):
        """
        Dump all media files that are reference by a FileField.
        """

        # Loop through all models and find FileFields
        for model in apps.get_models():

            # Get the name of all file fields in the model
            field_names = []
            for field in model._meta.fields:
                if isinstance(field, models.FileField):
                    field_names.append(field.name)

            # If any were found, loop through each row
            if len(field_names):
                for row in model.objects.all():
                    for field_name in field_names:
                        field = getattr(row, field_name)
                        if field:
                            self._add_file(tar, field)
                            field.close()

    def _dump_meta(self, tar):
        """
        Dump metadata to the archive.
        """
        data = MixedIO()
        meta_dict = OrderedDict((
            ('version', __version__),
            ('db_file', DB_DUMP),
            ('media_folder', MEDIA_DIR),
            ('settings', self.attr.settings_dict()),
        ))
        dump(meta_dict, data, indent=2)
        info = TarInfo(META_DUMP)
        info.size = data.rewind()
        tar.addfile(info, data)

    def _add_file(self, tar, file):
        info = TarInfo(path.join(MEDIA_DIR, file.name))
        info.size = file.size
        tar.addfile(info, file)
