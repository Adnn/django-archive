from django.conf import settings
from django.core.files.storage import DefaultStorage


DB_DUMP     = 'data.json'
META_DUMP   = 'meta.json'
MEDIA_DIR   = '_media'


def get_mediastorage():
    return DefaultStorage() # The default storage appears to be the one used for Media


class AttributeRepository(object):
    defaults = {
        'ARCHIVE_DIRECTORY': '',
        'ARCHIVE_FILENAME': 'django-archive_%Y-%m-%d--%H-%M-%S',
        'ARCHIVE_FORMAT': 'bz2',
        'ARCHIVE_EXCLUDE': (
            'auth.Permission',
            'contenttypes.ContentType',
            'sessions.Session',
        ),
        'ARCHIVE_DB_INDENT': None,
        'ARCHIVE_MEDIA_POLICY': 'all_files', #possible values: 'all_files', 'filefield_targets'
    }

    def get(self, name):
        return getattr(settings, name, self.defaults[name])

    def settings_dict(self):
        return {setting: self.get(setting) for setting in self.defaults}
