################################################################
### Text automatically added by daps-utils metaflowtask-init ###
from .__initplus__ import load_current_version, __basedir__, load_config
try:
    config = load_config()
except ModuleNotFoundError as exc:
    print(exc)
__version__ = load_current_version()
################################################################
