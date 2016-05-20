from .utils import *

from ... import __version__

from django.core.management import call_command
from django.core.management.base import BaseCommand

from json import load
from os import path
from tarfile import TarFile
import codecs, shutil, tempfile


class Command(BaseCommand):
    """
    Read an existing archive of database tables and uploaded media, and restore them as application data.
    """

    help = "Read an existing archive of database tables and uploaded media, and restore them as application data."

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.attr = AttributeRepository()
        self.stored_medias = []

    def add_arguments(self, parser):
        parser.add_argument('archive')

    def handle(self, *args, **options):
        """
        Process the command.
        """
        tar = self._open_archive(options)

        self.meta_dict = self._load_meta(tar)
        if self.meta_dict.get('version') > __version__:
            self.stderr.write("The archive version {} is superior to the command version {}: restoration aborted."
                             .format(self.meta_dict.get('version'), __version__))
            exit(1)

        try:
            self._load_files(tar)
            self._load_db(tar)
            self.stdout.write("Restoration completed.")
        except Exception as e:
            self.stderr.write("Aborted restoration as this exception occured: \n\t{}".format(e))
            if self.stored_medias:
                logpath = self._log_stored_files(options)
                self.stderr.write("The list of media files that were restored before failure was dumped to '{}'."
                                  .format(logpath))

        tar.close()

    def _open_archive(self, options):
        return TarFile.open(self.generated_filepath(options['archive']))

    def _load_meta(self, tar):
        # extractfile returns a readonly file-like object supporting read() of binary data, where json.load expects str.
        Reader = codecs.getreader("utf-8")
        return load(Reader(tar.extractfile(META_DUMP)))

    def _load_db(self, tar):
        # Note: The loaddata command has quite advanced logic that should be duplicated here, sadly this management
        # command can only get its input data from the filesystem, so we create a temporary file in order to use it.
        db_element = self.meta_dict.get('db_file')
        with tempfile.NamedTemporaryFile(suffix=".json") as temporary_extracted:
            # -1 is given as a negative value disables "looping over the source data in chunks", which was causing truncation
            shutil.copyfileobj(tar.extractfile(db_element), temporary_extracted, -1)
            call_command('loaddata', temporary_extracted.name)

    def _load_files(self, tar):
        media_storage = get_mediastorage()
        for media in [member for member in tar.getnames() if member.startswith(self.meta_dict.get('media_folder'))]:
            original_name   = path.relpath(media, self.meta_dict.get('media_folder'))
            stored_name     = media_storage.save(original_name, tar.extractfile(media))
            if original_name != stored_name:
                media_storage.delete(stored_name)
                raise Exception("The media '{}' was saved under a different name '{}'.".format(original_name, stored_name))
            else:
                self.stored_medias.append(stored_name)

    def _log_stored_files(self, options):
        logpath = self.generated_filepath("{}.log".format(options['archive']))
        with open(logpath, "w") as log:
            log.write("Files written to media storage:\n\t{}".format("\n\t".join(self.stored_medias))) 
        return logpath

    def generated_filepath(self, f_path):
        return f_path if path.isabs(f_path) else path.join(self.attr.get('ARCHIVE_DIRECTORY'), f_path) 
