from __future__ import print_function, division, absolute_import

import logging
from urllib.parse import urlparse

from .compatibility import check_output

format = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format=format, level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)


def set_logging(level):
    logger = logging.getLogger('knit')
    logger.setLevel(level)

set_logging('INFO')


def shell_out(cmd=None, **kwargs):
    """
    Thin layer on check_output to return data as strings

    Parameters
    ----------
    cmd : list
        command to run
    kwargs:
        passed directly to check_output

    Returns
    -------
    result : str
        result of shell command
    """
    return check_output(cmd, **kwargs).decode('utf-8')


def get_log_content(s):
    if 'Cannot find this log' in s:
        return ''
    st = """<td class="content">"""
    ind0 = s.find(st) + len(st)
    ind1 = s[ind0:].find("</td>")
    out = s[ind0:ind0+ind1]
    return out.lstrip('\n          <pre>').rstrip('</pre>\n        ')


def is_local(url):
    """
    Assume urls without scheme are local
    """
    parsed_url = urlparse(url)
    return not parsed_url.scheme or parsed_url.scheme == "file"


def url_to_path(url, hdfs=None):
    """
    From a url return a path (without scheme and host) for usage with hdfs
    This function will fail in case the hdfs filesystem is incorrect
    """
    parsed_url = urlparse(url)
    if hdfs and parsed_url.scheme != 'hdfs':
        raise RuntimeError("hdfs3 only supports hdfs filesystem")
    if hdfs and parsed_url.netloc != hdfs.host:
        raise RuntimeError("wrong filesystem: expected {} got {}".format(
            hdfs.host, parsed_url.netloc))
    return parsed_url.path
