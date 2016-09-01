#!/usr/bin/env python3
import ast
import json
import logging
import os
import re
import redis
import signal
import sys
import time
from zabbix_api import ZabbixAPI, ZabbixAPIException, Already_Exists


logging.basicConfig(filename='/var/log/zservice_server.log', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger('zservice')
for handler in logging.root.handlers:
    handler.addFilter(logging.Filter('zservice'))


class GracefulKiller:
    """Catch signals to allow graceful shutdown."""

    def __init__(self):
        self.receivedSignal = self.receivedTermSignal = False
        catchSignals = [1, 2, 3, 10, 12, 15]
        for signum in catchSignals:
            signal.signal(signum, self.handler)

    def handler(self, signum, frame):
        self.lastSignal = signum
        self.receivedSignal = True
        if signum in [2, 3, 15]:
            self.receivedTermSignal = True


def string2dict(string, object_type):
    """Converts string to dictionary and checks it for the list of keys"""
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
        logger.error('Wrong Item {}'.format(string))

        return False


def get_hostid_by_name(hostname):
    """Returns host ID for given hostname"""
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
    """Returns template ID for given name"""
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


def get_template_name_by_id(template_id):
    """Returns template name for given ID"""
    try:
        template = zapi.template.get({'templateids': template_id, 'output': ['host']})
    except ZabbixAPIException:
        templates_list = []

    if len(template) > 1:
        result = False
    elif len(template) == 0:
        result = False
    else:
        result = template[0]['host']

    return result


def get_parent_templates(template_id):
    """Recursive list of parent templates"""
    templates = []
    parent_templates_count = zapi.template.get({'templateids': template_id, 'selectParentTemplates':'count', 'output':['host', 'name']})[0]['parentTemplates']
    parent_templates_count = int(parent_templates_count)
    parent_templates = zapi.template.get({'templateids': template_id, 'selectParentTemplates':['host', 'name'], 'output':['host', 'name']})
    if parent_templates_count > 0:
        for pt in parent_templates[0]['parentTemplates']:
            templates.append(pt['name'])
            for t in (get_parent_templates(pt['templateid'])):
                templates.append(t)
    else:
        templates.append(parent_templates[0]['name'])

    return templates


def get_macros_for_host(host_id):
    """Get macros list for a host with host_id"""
    host_templates = []
    host_macros = {}

    # Get upper-level templates attached to a host
    for template in zapi.template.get({'hostids': host_id,
            'output': ['templateid']}):
        host_templates.append(template['templateid'])

    # Populate host_macros with upper-level macros on a host, if any
    for macro in zapi.usermacro.get({'hostids': host_id, 'output':
            ['macro', 'value']}):
        host_macros[macro['macro']] = macro['value']

    # Process upper-level templates recursively to get all macros
    for template in host_templates:
        template_macros = get_macros_for_template(template)
        template_macros.update(host_macros)
        host_macros = template_macros

    return host_macros


def get_macros_for_template(template_id):
    """Traverse template tree to get all macros values"""
    template_macros = {}

    # Populate template_macros with upper-level macros for template
    for macro in zapi.usermacro.get({'templateids': template_id, 'output':
            ['macro', 'value']}):
        template_macros[macro['macro']] = macro['value']

    # Zabbix calls template children its parents. Sigh...
    child_templates = zapi.template.get({'templateids': template_id,
            'selectParentTemplates': ['host', 'name'],
            'output': ['templateid']})[0]['parentTemplates']

    # Go deep
    for template in child_templates:
        child_macros = get_macros_for_template(template['templateid'])
        child_macros.update(template_macros)
        template_macros = child_macros

    return template_macros


def get_host_groupid_by_name(group):
    """Returns group ID for given host group (creates new one if not exist)"""
    host_group = zapi.hostgroup.get({'filter': {'name': group}, 'output': 'host'})

    if host_group:

        return host_group[0]['groupid']

    else:
        host_group = zapi.hostgroup.create({'name': group})

        return host_group[0]['groupids']


def find_duplicated_templates(templates_list):
    templates = {}
    found_in = {}
    all_templates = []

    for template in templates_list:
        template_name = get_template_name_by_id(template)
        all_templates.append(template_name)
        parent_templates = get_parent_templates(template)
        templates[template_name] = parent_templates
        all_templates.extend(parent_templates)

    duplicates = set([template for template in all_templates if all_templates.count(template) > 1])
    if duplicates:
        for template in duplicates:
            for parent in templates:
                if (parent == template) or (template in templates[parent]):
                    found_in[template] = parent

        return found_in


def create_zabbix_host(hostname):
    """Creates host in zabbix and returns its ID"""
    group = hostname.split('.')[1]
    group_id = get_host_groupid_by_name(group)

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
        logger.error('Something went wrong while creating {}'.format(hostname))

        return False

    logger.info ('Created host {0} with hostid {1}'.format(hostname, host['hostids'][0]))

    return host['hostids'][0]


def create_zabbix_item(item_dict, host_id):
    """Creates item (from dictionary) for given host (ID)"""
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
    """Creates trigger (from dictionary) """
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


def create_urotate_task_template(template_name, retention='2d'):
    """ Creates template for urotate task wih default exit code item and 2 triggers: status and nodata"""
    task_name = template_name.split('Template_Urotate_Tasks_')[-1]
    time_dict = {
        'd': 'days',
        'h': 'hours',
        'm': 'minutes',
        's': 'seconds'
    }
    dimension = time_dict[retention[-1]]
    ret_value = retention[:-1]

    if not ret_value.isdigit():
        logger.error('Wrong retention "{0}" for {1} task. Using default: 2 days'.format(retention, task_name))
        retention = '2d'
        dimension = 'days'
        ret_value = '2'

    groupids = [get_host_groupid_by_name('templates_'), get_host_groupid_by_name('Templates_Urotate')]
    try:
        template_id = zapi.template.create({'host': template_name, 'groups': [{'groupid': groupid} for groupid in groupids]})
    except ZabbixAPIException as e:
        logger.error('Can\'t create template for {0}'.format(task_name))

        return False

    template_id = template_id['templateids'][0]
    try:
        task_item = zapi.item.create({
            'delay': '0',
            'hostid': template_id,
            'interfaceid': '0',
            'key_': 'urotate.{0}.exit_status'.format(task_name),
            'name': 'Urotate task exit code: {0}'.format(task_name),
            'type': '2',
            'value_type': '3'
            })
    except ZabbixAPIException as e:
        zapi.template.delete([template_id])
        logger.info('{0}'.format(hostname, e))
        loger.error('Failed to create item "Urotate task exit code {0}" for {1}'.format(task_name, template_name))
        loger.error('Can\'t create template {0}'.format(template_name))

        return False

    itemid = task_item['itemids'][0]

    try:
        trigger=zapi.trigger.create({
            'description': 'HN006: Task {0} failed, check urotate task log'.format(task_name),
            'expression': '{{0}:urotate.{1}.exit_status.last(0)}<>0'.format(template_name, task_name),
            'comments': '',
            'priority': '4',
            'status': '0',
            'type': '0',
            'url': 'https://confluence.iponweb.net/pages/viewpage.action?pageId=51021521'
        })

        logger.info('Created trigger "Task {0} failed" with triggerid {1}'.format(task_name, trigger['triggerids']))

    except ZabbixAPIException as e:
        zapi.template.delete([template_id])
        logger.info('{0}'.format(hostname, e))
        loger.error('Failed to create trigger "Task {0} failed" for {1}'.format(task_name, template_name))
        loger.error('Can\'t create template {0}'.format(template_name))

        return False

    try:
        trigger=zapi.trigger.create({
            'description': 'HN003: No feedback from {0} task for {1} {2}. Make sure it works'.format(task_name, ret_value, dimension),
            'expression': '{{0}:urotate.{1}.exit_status.nodata({2})}=1'.format(template_name, task_name, retention),
            'comments': '',
            'priority': '4',
            'status': '0',
            'type': '0',
            'url': 'https://confluence.iponweb.net/pages/viewpage.action?pageId=50849973'
        })

        logger.info('Created trigger "Task {0} failed" with triggerid {1}'.format(task_name, trigger['triggerids']))

    except ZabbixAPIException as e:
        zapi.template.delete([template_id])
        logger.error('{0}'.format(hostname, e))
        loger.error('Failed to create trigger "No feedback from {0}" for {1}'.format(task_name, template_name))
        loger.error('Can\'t create template {0}'.format(template_name))

        return False

    return template_id


def process_urotate_macros(host_id, hostname):
    """ Gets list of nodata from redis, compares with current nodata macro in zabbix and updates it """
    redis_macro_list = {}
    current_nodata_macro_list = {}
    macro_task = {}
    zabbix_macro_list = zapi.usermacro.get({'hostids': host_id, 'output': ['macro', 'value']})
    host_key = '{0}:urotate_tasks'.format(hostname)
    keys = r.hkeys(host_key)

    for k in keys:
        k = k.decode('utf-8')
        nodata = r.hget(host_key, k)
        nodata = nodata.decode('utf-8')
        macro_name = '{$nodata_' + k + '}'
        macro_name = macro_name.upper()
        macro_name = macro_name.replace('-','_')
        redis_macro_list[macro_name] = nodata
        macro_task[macro_name] = k

    for macro in zabbix_macro_list:
        if '{$NODATA_' in macro['macro']:
            current_nodata_macro_list[macro['macro']] = macro['value']

    macro_2_remove = [m for m in current_nodata_macro_list.keys() if m not in redis_macro_list.keys()]
    macro_2_add = [m for m in redis_macro_list.keys() if m not in current_nodata_macro_list.keys()]
    macro_2_update = [m for m in redis_macro_list.keys() if m in current_nodata_macro_list.keys()]

    macro_id_2_remove = []
    for m in macro_2_remove:
        for zabbix_macro in zabbix_macro_list:
            if zabbix_macro['macro'] == m:
                macro_id = zabbix_macro['hostmacroid']
                macro_id_2_remove.append(macro_id)

    if macro_id_2_remove:
        logger.info('[{0}] Going to remove macros {1}'.format(hostname, macro_2_remove))
        zapi.usermacro.delete(macro_id_2_remove)

    for m in macro_2_update:
        for zabbix_macro in zabbix_macro_list:
            if zabbix_macro['macro'] == m:
                macro_id = zabbix_macro['hostmacroid']
                logger.info('[{0}] Going to update macro {1}'.format(hostname, m))
                zapi.usermacro.update({'hostmacroid': macro_id, 'value': redis_macro_list[m]})

    for m in macro_2_add:
        if check_nodata_macro_usage(macro_task[m]):
            logger.info('[{0}] Going to add macro {1} with value {2}'.format(hostname, m, redis_macro_list[m]))
            zapi.usermacro.create({'hostid': host_id, 'macro': m, 'value': redis_macro_list[m]})


def process_zabbix_macro(host_id, hostname):
    """ Gets list of macro for host from redis and updates it (doesn't touch urotate_nodata macro)"""
    redis_macro_list = {}
    current_macro_list = {}
    macro_task = {}
    zabbix_macro_list = zapi.usermacro.get({'hostids': host_id, 'output': ['macro', 'value']})
    host_key = '{0}:zabbix_macro'.format(hostname)
    keys = r.hkeys(host_key)

    for k in keys:
        k = k.decode('utf-8')
        macro_value = r.hget(host_key, k)
        macro_value = macro_value.decode('utf-8')
        macro_name = '{$' + k + '}'
        macro_name = macro_name.upper()
        macro_name = macro_name.replace('-','_')
        redis_macro_list[macro_name] = macro_value

    for macro in zabbix_macro_list:
        if '{$NODATA_' not in macro['macro']:
                current_macro_list[macro['macro']] = macro['value']

    macro_2_remove = [m for m in current_macro_list.keys() if m not in redis_macro_list.keys()]
    macro_2_add = [m for m in redis_macro_list.keys() if m not in current_macro_list.keys()]
    macro_2_update = [m for m in redis_macro_list.keys() if m in current_macro_list.keys()]

    macro_id_2_remove = []
    for m in macro_2_remove:
        for zabbix_macro in zabbix_macro_list:
            if zabbix_macro['macro'] == m:
                macro_id = zabbix_macro['hostmacroid']
                macro_id_2_remove.append(macro_id)

    if macro_id_2_remove:
        logger.info('[{0}] Going to remove macros {1}'.format(hostname, macro_2_remove))
        zapi.usermacro.delete(macro_id_2_remove)

    for m in macro_2_update:
        for zabbix_macro in zabbix_macro_list:
            if zabbix_macro['macro'] == m:
                macro_id = zabbix_macro['hostmacroid']
                logger.info('[{0}] Going to update macro {1}'.format(hostname, m))
                zapi.usermacro.update({'hostmacroid': macro_id, 'value': redis_macro_list[m]})

    for m in macro_2_add:
        logger.info('[{0}] Going to add macro {1} with value {2}'.format(hostname, m, redis_macro_list[m]))
        zapi.usermacro.create({'hostid': host_id, 'macro': m, 'value': redis_macro_list[m]})


def check_nodata_macro_usage(template_name):

    if 'Template_Urotate_Tasks_' not in template_name:

        return False

    task_name = template_name.split('Template_Urotate_Tasks_')[-1]
    macro_name = '{$nodata_' + task_name + '}'
    macro_name = macro_name.upper()
    macro_name = macro_name.replace('-','_')    

    template_triggers = zapi.template.get({'filter': {'name': template_name}, 'selectTriggers': ['triggerid'], 'output': ['triggers']})
    template_triggers = template_triggers[0]['triggers']

    for trigger in template_triggers:
        trigger_id = trigger['triggerid']
        function = zapi.trigger.get({'triggerids': trigger_id, 'selectFunctions': 'extend', 'output': 'functions'})
        parameter = function[0]['functions'][0]['parameter']
        if macro_name in parameter:

            return True

    return False



def chek_task_monitoring(template_name, host_id):
    task_name = template_name.split('Template_Urotate_Tasks_')[-1]
    task_item = 'urotate.{0}.exit_status'.format(task_name)
    host_items = zapi.host.get({'hostids': host_id, 'selectItems': ['key_']})
    host_items = host_items[0]['items']

    for item in host_items:
        if task_item in item['key_']:

            return True

    return False


def assign_zabbix_templates(templates_names, host_id, hostname):
    """Assigns zabbix templates to host"""
    templates_list = []
    urotate_templates_list = []
    current_templates = zapi.template.get({'hostids': host_id, 'output': 'templateid'})
    for template in current_templates:
        templates_list.append(template['templateid'])

    for template_name in templates_names:

        if 'Template_Urotate_Tasks' in template_name:
            urotate_templates_list.append(template_name)
            continue

        logger.info('[{0}] Going to assign template {1}'.format(hostname, template_name))
        template_id = get_templateid_by_name(template_name)

        if not template_id:
            logger.warning ('[{0}] Template {1} not found'.format(hostname, template_name))
            continue

        if template_id not in templates_list:
            templates_list.append(template_id)
            try:
                zapi.host.update({'hostid': host_id, 'templates': templates_list})   
            except ZabbixAPIException as e:
                duplicate_error = 'cannot be linked to another template more than once'
                if duplicate_error in e.args[0]:  
                    logger.error('[{0}] Duplicated templates, please find and fix (caused by assigning "{1}")'.format(hostname, template_name))
                else:
                    logger.error('[{0}] {1}'.format(hostname, e))

    for template_name in urotate_templates_list:

        template_id = get_templateid_by_name(template_name)

        if template_id and (template_id not in templates_list):
            templates_list.append(template_id)
            try:
                zapi.host.update({'hostid': host_id, 'templates': templates_list})   
            except ZabbixAPIException as e:
                duplicate_error = 'cannot be linked to another template more than once'
                if duplicate_error in e.args[0]:  
                    logger.error('[{0}] Duplicated templates, please find and fix (caused by assigning "{1}")'.format(hostname, template_name))
                else:
                    logger.error('[{0}] {1}'.format(hostname, e))

        elif template_id:
            logger.info('[{0}] Template {1} already asigned to host'.format(hostname, template_name))

        elif not template_id:

            if chek_task_monitoring(template_name, host_id):
                logger.info('[{0}] Urotate task template {1} not found, but task already has monitoring'.format(hostname, template_name))
                continue

            logger.info ('[{0}] Urotate task template {1} not found, going to create it'.format(hostname, template_name))
            template_id = create_urotate_task_template(template_name)
            
            if not template_id:
                continue

            templates_list.append(template_id)
            try:
                zapi.host.update({'hostid': host_id, 'templates': templates_list})   
            except ZabbixAPIException as e:
                duplicate_error = 'cannot be linked to another template more than once'
                if duplicate_error in e.args[0]:  
                    logger.error('[{0}] Duplicated templates, please find and fix (caused by assigning "{1}")'.format(hostname, template_name))
                else:
                    logger.error('[{0}] {1}'.format(hostname, e))


def fix_zabbix_templates(templates_names, host_id, hostname):
    """Replaces zabbix template with given list"""
    templates_names_map = {}
    current_templates_list = []
    templates_list = []
    urotate_templates = []
    templates_2_add = []
    templates_2_remove = []
    
    current_templates = zapi.template.get({'hostids': host_id, 'output': ['templateid', 'name']})
    for template in current_templates:
        current_templates_list.append(template['templateid'])
        templates_names_map[template['templateid']] = template['name']


    for template_name in templates_names:

        if 'Template_Urotate_Tasks' in template_name:
            urotate_templates.append(template_name)
            continue

        template_id = get_templateid_by_name(template_name)

        if not template_id:
            logger.info ('[{0}] Template {1} not found'.format(hostname, template_name))
            continue

        templates_names_map[template_id] = template_name
        if template_id not in templates_list:
            templates_list.append(template_id)
        else:
            logger.info('[{0}] Template {0} already asigned to host'.format(hostname, template_name))

    for template_name in urotate_templates:
        template_id = get_templateid_by_name(template_name)

        if not template_id:
            logger.info ('[{0}] Template {1} not found'.format(hostname, template_name))
            continue

        templates_names_map[template_id] = template_name
        if template_id not in templates_list:
            templates_list.append(template_id)
        else:
            logger.info('[{0}] Template {0} already asigned to host'.format(hostname, template_name))
    
    for template in templates_list:
        if template not in current_templates_list:
            templates_2_add.append(template)

    for template in current_templates_list:
        if template not in templates_list:
            templates_2_remove.append(template)

    templates_names_2_remove = []
    templates_names_2_add = []

    for template in templates_2_remove:
        templates_names_2_remove.append(templates_names_map[template])

    for template in templates_2_add:
        templates_names_2_add.append(templates_names_map[template])

    if templates_2_remove:
        try:
            logger.info('[{0}] Going to remove templates {1}'.format(hostname, templates_names_2_remove))

            return zapi.host.update({'hostid': host_id, 'templates_clear': templates_2_remove})

        except ZabbixAPIException as e:
            logger.error('[{0}] {1}'.format(hostname, e))

            return False

    current_templates_list = []
    current_templates = zapi.template.get({'hostids': host_id, 'output': 'templateid'})
    for template in current_templates:
        current_templates_list.append(template['templateid'])

    logger.info('[{0}] Will try to add this templates: {1}'.format(hostname, templates_names_2_add))
    for template in templates_2_add:
        try:
            logger.info('[{0}] Going to assign template {1}'.format(hostname, templates_names_map[template]))
            current_templates_list.append(template)
            zapi.host.update({'hostid': host_id, 'templates': current_templates_list})

        except ZabbixAPIException as e:
            duplicate_error = 'cannot be linked to another template more than once'
            if duplicate_error in e.args[0]:
                logger.error('[{0}] Duplicated templates, please find and fix (caused by assigning "{1}")'.format(hostname, template_name))
            else:
                logger.error('[{0}] {1}'.format(hostname, e))

            return False


def remove_zabbix_template(template_name, host_id, hostname):
    """Assigns zabbix template to host"""
    logger.info('[{0}] Going to remove template {1}'.format(hostname, template_name))

    template_id = get_templateid_by_name(template_name)
    if not template_id:
        logger.info ('[{0}] Template {1} not found'.format(hostname, template_name))

        return False

    try:
        
        return zapi.host.update({'hostid': host_id, 'templates_clear': template_id})

    except ZabbixAPIException as e:
        logger.error('[{0}] {1}'.format(hostname, e))

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
        logger.error(e)
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

        assign_zabbix_templates(templates_list, host_id, hostname)

        return None

    if modification_type == 'urotate_tasks' and host_id:
        process_urotate_macros(host_id, hostname)

        return None

    if modification_type == 'zabbix_macro' and host_id:
        process_zabbix_macro(host_id, hostname)

        return None

    if regular:
        if modification_type == 'templates':
            templates_list = []
            for string in regular:
                string = string.decode('utf-8')
                templates_list.append(string)

        fix_zabbix_templates(templates_list, host_id, hostname)

        return None


    if removed and host_id:
        for string in removed:
            string = string.decode('utf-8')

            if modification_type == 'templates':
                remove_zabbix_template(string, host_id, hostname)
            else:
                continue

    if added and host_id:
        if modification_type == 'templates':
            templates_list = []
            for string in added:
                string = string.decode('utf-8')
                templates_list.append(string)
            assign_zabbix_templates(templates_list, host_id, hostname)

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
    server_config = '/etc/zservice/zservice_server.conf'
    if os.path.isfile (server_config):
        with open(server_config) as f:
            configuration = json.load(f)
    else:
        logger.error('Configuration file {} not found'.format(server_config))
        sys.exit()

    try:
        zabbix_server = configuration['zabbix_server']
        zabbix_login = configuration['zabbix_login']
        zabbix_password = configuration['zabbix_password']
        redis_host = configuration['redis_host']
        redis_port = configuration['redis_port']
    except KeyError as e:
        logger.error('Not found in config: {}'.format(e))
        sys.exit()

    zapi = ZabbixAPI(zabbix_server)
    zapi.login(zabbix_login, zabbix_password)
    logger.info('Connected to Zabbix API Version {}'.format(zapi.api_version()))

    killer = GracefulKiller()

    r = redis.Redis(host=redis_host, port=redis_port, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe(**{'notify-channel': Redis_handler})
    logger.info(p.get_message())

    while True:
        if killer.receivedSignal:
            if killer.receivedTermSignal:
                logging.warning("Gracefully exiting due to receipt of signal {}".format(killer.lastSignal))
                sys.exit()
            else:
                logging.warning("Ignoring signal {}".format(killer.lastSignal))
                killer.receivedSignal = killer.receivedTermSignal = False

        message = p.get_message()
        time.sleep(0.001) 
