################################################################
# Text automatically added by daps-utils metaflowtask-init     #
from .__initplus__ import load_current_version, __basedir__, load_config
try:
    config = load_config()
except ModuleNotFoundError as exc:
    print(exc)
__version__ = load_current_version()
################################################################


def declarative_base(prefix=""):
    from sqlalchemy.ext.declarative import declarative_base, declared_attr
    import re

    def camel_to_snake(str_):
        return re.sub(r'((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))', r'_\1', str_).lower()

    class _Base(object):
        """ Research DAPS Base object"""
        @declared_attr
        def __tablename__(cls):
            return prefix + camel_to_snake(cls.__name__)

    return declarative_base(cls=_Base)
