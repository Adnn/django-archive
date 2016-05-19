from collections import OrderedDict
from datetime import datetime
from io import BytesIO
from json import dump
from os import path
from tarfile import TarInfo, TarFile

from django.apps.registry import apps
from django.conf import settings
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


class Command(BaseCommand):
    """
    Create a compressed archive of database tables and uploaded media.
    """

    help = "Create a compressed archive of database tables and uploaded media."

    def handle(self, *args, **kwargs):
        """
        Process the command.
        """
        self.attr = AttributeRepository()

        tar = self._create_archive()
        self._dump_db(tar)
        self._dump_files(tar)
        self._dump_meta(tar)
        self.stdout.write("Backup completed to archive '{}'.".format(tar.name))
        tar.close()

    def _create_archive(self):
        """
        Create the archive and return the TarFile.
        """
        filename = self.attr.get('ARCHIVE_FILENAME')
        fmt = self.attr.get('ARCHIVE_FORMAT')
        absolute_path = path.join(
            self.attr.get('ARCHIVE_DIRECTORY'),
            '%s.tar.%s' % (datetime.today().strftime(filename), fmt)
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
        """
        Dump all uploaded media to the archive.
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
                            info = TarInfo(field.name)
                            info.size = field.size
                            tar.addfile(info, field)
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
