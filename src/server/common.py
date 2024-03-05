from icecream import ic

from consts import *

ic.configureOutput(prefix=f'[{HOSTNAME}: {SERVER_ID}] | ')

# Disable icecream debug messages if DEBUG is not set to true
if not DEBUG:
    ic.disable()