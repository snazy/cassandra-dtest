import fileinput
import glob
import os
import re
import shutil
import sys
import tempfile

from dtests.dtest import debug  # Depending on dtest is not good long-term.


def replace_in_file(filepath, search_replacements):
    """
    In-place file search and replace.

    filepath - The path of the file to edit
    search_replacements - a list of tuples (regex, replacement) that
    represent however many search and replace operations you wish to
    perform.

    Note: This does not work with multi-line regexes.
    """
    for line in fileinput.input(filepath, inplace=True):
        for regex, replacement in search_replacements:
            line = re.sub(regex, replacement, line)
        sys.stdout.write(line)


def safe_mkdtemp():
    tmpdir = tempfile.mkdtemp()
    # \ on Windows is interpreted as an escape character and doesn't do anyone any favors
    return tmpdir.replace('\\', '/')


def size_of_files_in_dir(dir_name, verbose=True):
    """
    Return the size of all files found in a non-recursive ls of the argument.
    Based on http://stackoverflow.com/a/1392549
    """
    files = [os.path.join(dir_name, f) for f in os.listdir(dir_name)]
    if verbose:
        debug('getting sizes of these files: {}'.format(files))
    return sum(os.path.getsize(f) for f in files)


def get_snapshot_dirs(node, ks, cf):
    snapshot_dirs = []
    for data_dir in node.data_directories():
        for pattern in ["{data_dir}/{ks}/{cf}/snapshots/", "{data_dir}/{ks}/{cf}-*/snapshots/"]:
            snapshot_dirs += glob.glob(pattern.format(data_dir=data_dir, ks=ks, cf=cf))
    return snapshot_dirs


def clear_snapshot_dir(node, ks, cf):
    for dir in get_snapshot_dirs(node, ks, cf):
        debug("Removing snapshot dir: {}".format(str(dir)))
        shutil.rmtree(dir)


def has_snapshot_dir(node, ks, cf):
    for dir in get_snapshot_dirs(node, ks, cf):
        debug("Found snapshot dir: {}".format(str(dir)))
        return True
    debug("No snapshot found")
    return False
