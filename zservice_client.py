#!/usr/bin/env python3
import difflib
import filecmp
import json
import logging
import os
import redis
import shutil
import socket
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

monitoring_dir = '/opt/zservice/'
previous_dir = '/opt/zservice/prev'
replaces_dir = '/opt/zservice/replace'

logging.basicConfig(filename='/var/log/zservice_client.log', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('zservice')
for handler in logging.root.handlers:
    handler.addFilter(logging.Filter('zservice'))


class file_changes(FileSystemEventHandler):

    def on_modified(self, event):
        if not event.is_directory and ('items' in event.src_path) and ('lock' not in event.src_path): 
            process_changes('items')

        elif not event.is_directory and ('triggers' in event.src_path) and ('lock' not in event.src_path):
            process_changes('triggers')

        elif not event.is_directory and ('templates' in event.src_path) and ('lock' not in event.src_path) and ('replacement' not in event.src_path):
            process_changes('templates')

        elif not event.is_directory and ('templates_replacements' in event.src_path) and ('lock' not in event.src_path):
            regular_check('templates')



def process_changes(item_type):
    """ Cheks modified file for content changes and pushes them to redis """
    modified_file = item_type
    prev_file_path = os.path.join(previous_dir, modified_file + '.previous')
    source_path = os.path.join(monitoring_dir, modified_file)

    logger.info('Got changes in {}'.format(source_path))
    if not config_not_modified(prev_file_path, source_path):
        if os.path.isfile(prev_file_path):
            logger.info ('Config modified!')
            new, removed = files_diff(prev_file_path, source_path)
            full = file2list(source_path)
            new = process_replacements(new, item_type)
            removed = process_replacements(removed, item_type)
            full = process_replacements(full, item_type)

            push_list2redis(new, item_type, 'new')
            push_list2redis(removed, item_type, 'removed')
            push_list2redis(full, item_type, 'full')
            push_list2redis([], item_type, 'regular')
            notify_backend(item_type)
        else:
            new = file2list(source_path)
            new = process_replacements(new, item_type)

            push_list2redis(new, item_type, 'new')
            push_list2redis([], item_type, 'removed')
            push_list2redis(new, item_type, 'full')
            push_list2redis([], item_type, 'regular')
            notify_backend(item_type)
    else:
        full = file2list(source_path)
        full = process_replacements(full, item_type)

        push_list2redis([], item_type, 'new')
        push_list2redis([], item_type, 'removed')
        push_list2redis(full, item_type, 'full')
        push_list2redis([], item_type, 'regular')
        notify_backend(item_type)

    shutil.copyfile(source_path, prev_file_path)


def process_replacements(item_list, item_type):
    """ Aplies custom config modifications to the items list """
    item_list_replaced = []
    conf_file = os.path.join(monitoring_dir,item_type + '_replacements.json')

    if not os.path.isfile(conf_file):

        return item_list

    with open(conf_file) as f:
        replace_settings = json.load(f)

    for item in item_list:
        if item in replace_settings:
            item_list_replaced.append(replace_settings[item])
        else:
            item_list_replaced.append(item)

    return item_list_replaced



def regular_check(item_type):
    """ Pushes current file to redis and notifies server """
    modified_file = item_type
    prev_file_path = os.path.join(previous_dir, modified_file + '.previous')
    source_path = os.path.join(monitoring_dir, modified_file)

    logger.info('Starting regular check for {}'.format(source_path))
    regular = file2list(source_path)
    regular = process_replacements (regular, item_type)

    push_list2redis([], item_type, 'new')
    push_list2redis([], item_type, 'removed')
    push_list2redis(regular, item_type, 'full')
    push_list2redis(regular, item_type, 'regular')
    notify_backend(item_type)


def comp_lists(list1, list2):
    """ Returns items from list1 not presented in list """
    not_assigned = []
    for v in list1:
        if v not in list2:
            not_assigned.append(v)
    
    return not_assigned


def files_diff(old_file, new_file):
    """ Returns new lines and removed lines """
    with open(old_file) as f:
        old_lines = [line.rstrip('\n') for line in f]

    with open(new_file) as f:
        new_lines = [line.rstrip('\n') for line in f]

    diff = difflib.unified_diff(old_lines, new_lines, fromfile=old_file, tofile=new_file, lineterm='', n=0)
    lines = list(diff)[2:]
    added = [line[1:] for line in lines if line[0] == '+']
    removed = [line[1:] for line in lines if line[0] == '-']
    added_uniq = comp_lists(added, removed)
    removed_uniq = comp_lists(removed, added)

    return  added_uniq, removed_uniq


def config_not_modified(old_file, new_file):
    """ Cheks if file have actual changes"""
    if os.path.isfile(old_file):

        return filecmp.cmp(new_file, old_file, shallow=False)  

    else:

        return False


def file2list(file):
    """ Returns list made by file strings """
    with open(file) as f:
        lines = [line.rstrip('\n') for line in f]

    return lines


def push_list2redis(pushlist, item_type, state, clean=True):
    """ pushes list of items to redis """
    hostname = socket.gethostname()
    pipe = r.pipeline()
    key = hostname + ':' + item_type + ':' + state
    if clean:
        pipe.delete(key)
    for line in pushlist:
        pipe.sadd(key, line)

    pipe.execute()


def notify_backend(item_type):
    """ sends notification to redis chanel """
    hostname = socket.gethostname()
    r.publish('notify-channel','{0} {1}'.format(hostname, item_type))
        

if __name__ == "__main__":
    time_counter = 0

    client_config = '/etc/zservice/zservice_client.conf'
    if os.path.isfile (client_config):
        with open(client_config) as f:
            configuration = json.load(f)
    else:
        logger.error('Configuration file {} not found'.format(client_config))
        sys.exit()

    try:
        redis_host = configuration['redis_host']
        redis_port = configuration['redis_port']
    except KeyError as e:
        logger.error('Not found in config: {}'.format(e))
        sys.exit()

    r = redis.Redis(host=redis_host, port=redis_port, db=0)
    event_handler = file_changes()
    observer = Observer()
    observer.schedule(event_handler, path=monitoring_dir, recursive=False)
    observer.start()

    try:
        while True:
            time_counter += 0.01
            if time_counter > 60:
                regular_check('templates')
                time_counter = 0

            time.sleep(0.01)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()