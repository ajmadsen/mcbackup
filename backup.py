import os
import subprocess
from datetime import datetime
from os.path import join

import inotify.adapters
import inotify.constants

BACKUPS_PATH = "/backups"
MC_ROOT = "/mc"
BUCKET_PATH = "s3://thot-zone/craft.thot.zone/backups/"
CONTAINER_NAME = "mc"
TIMEOUT = 10
RETRIES = 5


def rcon(*cmd):
    return subprocess.check_call(
        ['docker', 'exec', CONTAINER_NAME, 'rcon-cli', *cmd])


def backup(retries=RETRIES):
    filename = 'mc.{:%Y%m%d%H%M%S}.tar'.format(datetime.now())
    tar_path = join(BACKUPS_PATH, filename)
    for _ in range(retries):
        try:
            print('Archiving...')
            subprocess.check_call(['tar', 'uvf', tar_path, MC_ROOT])
            print('Compressing...')
            subprocess.check_call(['gzip', tar_path])
            return tar_path + '.gz'
        except:
            pass

    os.unlink(tar_path)
    raise RuntimeError('could not archive {}'.format(MC_ROOT))


def upload(tar_path, unlink_on_success=False):
    subprocess.check_call(['aws', 's3', 'cp', tar_path, BUCKET_PATH])
    if unlink_on_success:
        print('Unlinking backup')
        os.unlink(tar_path)


def do_save():
    try:
        print('Disabling autosave')
        rcon('save-off')
        watcher = inotify.adapters.InotifyTree(MC_ROOT, mask=(
            inotify.constants.IN_MODIFY | inotify.constants.IN_CLOSE_WRITE))

        rcon('save-all')
        print('Waiting for world to flush')
        for _ in watcher.event_gen(yield_nones=False, timeout_s=TIMEOUT):
            pass
        print('Events are done')

        print('Creating archive')
        tar_path = backup()

        print('Uploading')
        upload(tar_path, unlink_on_success=True)
    finally:
        print('Restoring autosave')
        rcon('save-on')


def main():
    do_save()


if __name__ == '__main__':
    main()
