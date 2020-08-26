import re
from enum import Enum

VERIFY_FILE_HEAD = '{"meta":{"v":"2.0"'
TITLE_RE = re.compile('<title>(.*?)</title>')
HEADER = {
    'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
}


class GrabbingFilter(Enum):
    OWNER = 'OWNER'
    OPPONENT = 'OPPONENT'
    PAIR = 'PAIR'
    ALL_EXCEPT_PAIR = 'ALL_EXCEPT_PAIR'
    ALL = 'ALL'
