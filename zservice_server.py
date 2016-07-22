#!/usr/bin/env python3
import ast
import logging
import re
import redis
import sys
import time
from zabbix_api import ZabbixAPI, ZabbixAPIException, Already_Exists


logging.basicConfig(filename='/var/log/zservice_server.log', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('zservice')
for handler in logging.root.handlers:
    handler.addFilter(logging.Filter('zservice'))


def string2dict(string, object_type):
    '''Converts string to dictionary and checks it for the list of keys'''
    if object_type == 'items':
        keys = ['name', 'key_', 'type', 'value_type', 'delay']
    elif object_type == 'triggers':
        keys = ['description', 'expression', 'comments', 'priority', 'status', 'type', 'url']
    try:
        result = ast.literal_eval(string)
        if (isinstance(result, dict)) and (all (key in result for key in keys)):
            
            return result
        else:
            logger.info('Wrong Item {}'.format(string))

            return False
    except:
        logger.info('Wrong Item {}'.format(string))

        return False


def get_hostid_by_name(hostname):
    '''Returns host ID for given hostname'''
    hostid = zapi.host.get({'filter':{'host': hostname}, 'output':['host']})
    if len(hostid) > 1:
        raise Exception('More then one host with such name: {0}'.format(hostname))
    elif len(hostid) == 0:
        logger.info('No hosts with such name: {0}\nGoing to create it.'.format(hostname))
        result = create_zabbix_host(hostname)
        host_is_new = True

    else:
        result = hostid[0]['hostid']
        host_is_new = False

    return result, host_is_new


def get_templateid_by_name(template_name):
    '''Returns template ID for given name'''
    try:
        template = zapi.template.get({'filter': {'host': template_name}, 'output': 'templateid'})
    except ZabbixAPIException:
        templates_list = []

    if len(template) > 1:
        result = False
    elif len(template) == 0:
        result = False
    else:
        result = template[0]['templateid']

    return result


def get_groupid_by_name(group):
    '''Returns group ID for given groupname'''
    group = zapi.hostgroup.get({'filter': {'name': group}, 'output': 'host'})

    return group[0]['groupid']


def create_zabbix_host(hostname):
    '''Creates host in zabbix and returns its ID'''
    group = hostname.split('.')[1]
    group_id = get_groupid_by_name(group)

    try:
        host = zapi.host.create({
            'host': hostname,
            'interfaces': [{
                'type': 1,
                'main': 1,
                'useip': 0,
                'ip': '',
                'dns': hostname,
                'port': '10050'
            }],
            'groups': [{'groupid': group_id}]
            })
    except:
        logger.info('Something went wrong while creating {}'.format(hostname))

        return False

    logger.info ('Created host {0} with hostid {1}'.format(hostname, host['hostids'][0]))

    return host['hostids'][0]


def create_zabbix_item(item_dict, host_id):
    '''Creates item (from dictionary) for given host (ID)'''
    host_interface = zapi.hostinterface.get({'filter':{'hostid': host_id}})
    if host_interface:
        interface = host_interface[0]["interfaceid"]
    else:
        logger.info("no interface found")

        return False

    try:
        item=zapi.item.create({
            'hostid': host_id,
            'name': item_dict['name'],
            'key_': item_dict['key_'],
            'interfaceid': interface,
            'type': item_dict['type'],
            'value_type': item_dict['value_type'],
            'delay': item_dict['delay']
        })

        logger.info('Added item with itemid {0}'.format(item['itemids']))

    except Already_Exists as e:
        item=zapi.item.get({
            'hostids': host_id,
            'filter': {
                'name': item_dict['name'],
                'key_': item_dict['key_']
            },
            'output': 'extend',
        })
        item_id = item[0]['itemid']

        item=zapi.item.update({
            'itemid': item_id,
            'name': item_dict['name'],
            'key_': item_dict['key_'],
            'interfaceid': interface,
            'type': item_dict['type'],
            'value_type': item_dict['value_type'],
            'delay': item_dict['delay']
        })
        logger.info('Item with itemid {0} has been updated'.format(item_id))


def create_zabbix_trigger(trigger_dict, hostname):
    '''Creates trigger (from dictionary) '''
    expression = re.sub ('HOST_PLACEHOLDER', hostname, trigger_dict['expression'])
    try:
        trigger=zapi.trigger.create({
            'description': trigger_dict['description'],
            'expression': expression,
            'comments': trigger_dict['comments'],
            'priority': trigger_dict['priority'],
            'status': trigger_dict['status'],
            'type': trigger_dict['type'],
            'url': trigger_dict['url']
        })

        logger.info('Added trigger with triggerid {0}'.format(trigger['triggerids']))

    except Already_Exists as e:
        trigger=zapi.trigger.get({
            'expandExpression': 1,
            'host': hostname,
            'filter': {
                'description': trigger_dict['description']
            },
            'output': 'extend',
        })
        trigger_id = trigger[0]['triggerid']

        trigger=zapi.trigger.update({
            'triggerid': trigger_id,
            'description': trigger_dict['description'],
            'expression': expression,
            'comments': trigger_dict['comments'],
            'priority': trigger_dict['priority'],
            'status': trigger_dict['status'],
            'type': trigger_dict['type'],
            'url': trigger_dict['url']
        })
        logger.info('Trigger with triggerid {0} has been updated'.format(trigger_id))


def assign_zabbix_templates(templates_names, host_id):
    '''Assigns zabbix template to host'''
    templates_list = []
    current_templates = zapi.template.get({'hostids': host_id, 'output': 'templateid'})
    for template in current_templates:
        templates_list.append(template['templateid'])

    for template_name in templates_names:
        logger.info('Going to assing template {}'.format(template_name))
        templateid = get_templateid_by_name(template_name)
        if not templateid:
            logger.info ('Template {0} not found'.format(template_name))
            continue

        if templateid not in templates_list:
            templates_list.append(templateid)
        else:
            logger.info('Template {0} already asigned to host'.format(template_name))

    try:
        
        return zapi.host.update({'hostid': host_id, 'templates': templates_list})

    except ZabbixAPIException as e:
        logger.info(e)

        return False


def fix_zabbix_templates(templates_names, host_id):
    '''Replaces zabbix template with given list'''
    templates_list = []

    for template_name in templates_names:
        logger.info('Going to assing template {}'.format(template_name))
        templateid = get_templateid_by_name(template_name)
        if not templateid:
            logger.info ('Template {0} not found'.format(template_name))
            continue

        if templateid not in templates_list:
            templates_list.append(templateid)
        else:
            logger.info('Template {0} already asigned to host'.format(template_name))

    try:
        
        return zapi.host.update({'hostid': host_id, 'templates': templates_list})

    except ZabbixAPIException as e:
        logger.info(e)

        return False

def remove_zabbix_template(template_name, host_id):
    '''Assigns zabbix template to host'''
    logger.info('Going to remove template {}'.format(template_name))

    templateid = get_templateid_by_name(template_name)
    if not templateid:
        logger.info ('Template {0} not found'.format(template_name))

        return False

    try:
        
        return zapi.host.update({'hostid': host_id, 'templates_clear': templateid})

    except ZabbixAPIException as e:
        logger.info(e)

        return False


def create_zabbix_item_mult(items_list, host_id):
    host_interface = zapi.hostinterface.get({'filter':{'hostid': host_id}})
    if host_interface:
        interface = host_interface[0]["interfaceid"]
    else:
        logger.info("No interface found")

        return False
    for item in items_list:
        item['hostid'] = host_id
        item['interfaceid'] = interface
    try:
        items=zapi.item.create(items_list)

        logger.info("Added item with itemid {0}".format(items["itemids"]))

    except ZabbixAPIException as e:
        logger.info(e)
        #logger.info("Item {} hasn't been created".format(item_dict["key_"]))


def Redis_handler(message):
    message_data = message['data'].decode('utf-8')
    message_data = message_data.split()
    hostname = message_data[0]
    modification_type = message_data[1]
    key_new = hostname + ':' + modification_type + ':new'
    key_removed = hostname + ':' + modification_type + ':removed'
    key_full = hostname + ':' + modification_type + ':full'
    key_regular = hostname + ':' + modification_type + ':regular'
    added = r.sinter(key_new)
    removed = r.sinter(key_removed)
    full = r.sinter(key_full)
    regular = r.sinter(key_regular)

    try:
        host_id, new_host   = get_hostid_by_name(hostname)
    except Exception as e:
        host_id = False
        hostname = False
        logger.info(e)

        return None
    
    if new_host:
        if modification_type == 'templates':
            templates_list = []
            for string in full:
                string = string.decode('utf-8')
                templates_list.append(string)

        assign_zabbix_templates(templates_list, host_id)

        return None

    if regular:
        if modification_type == 'templates':
            templates_list = []
            for string in regular:
                string = string.decode('utf-8')
                templates_list.append(string)

        fix_zabbix_templates(templates_list, host_id)

        return None


    if removed and host_id:
        for string in removed:
            string = string.decode('utf-8')

            if modification_type == 'templates':
                remove_zabbix_template(string, host_id)
            else:
                continue

    if added and host_id:
        if modification_type == 'templates':
            templates_list = []
            for string in added:
                string = string.decode('utf-8')
                templates_list.append(string)
            assign_zabbix_templates(templates_list, host_id)

        else:
            for string in added:
                string = string.decode('utf-8')
                item_dict = string2dict(string, modification_type)
                if item_dict:
                    if modification_type == 'items':
                        create_zabbix_item(item_dict, host_id)
                    elif modification_type == 'triggers' and hostname:
                        create_zabbix_trigger(item_dict, hostname)
                else:
                    continue


if __name__ == "__main__":
    zapi = ZabbixAPI("https://zabbix.zabbix")
    zapi.login("login", "pass")
    logger.info ("Connected to Zabbix API Version %s"%zapi.api_version())

    r = redis.Redis(host='redis', port=6379, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe(**{'notify-channel': Redis_handler})
    logger.info(p.get_message())

    while True:
        message = p.get_message()

        time.sleep(0.001) 
