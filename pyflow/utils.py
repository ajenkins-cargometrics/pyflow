import json
import uuid


def encode_task_input(input_obj):
    return json.dumps(input_obj)


def decode_task_result(result_str):
    if result_str is None:
        return None
    else:
        return json.loads(result_str)


def iter_collection(paginator, items_key):
    for page in paginator:
        for item in page[items_key]:
            yield item


def list_domains(swf):
    paginator = swf.get_paginator('list_domains')
    return iter_collection(
        paginator.paginate(registrationStatus='REGISTERED'), 'domainInfos')


def list_workflow_types(swf, domain):
    paginator = swf.get_paginator('list_workflow_types')
    return iter_collection(
        paginator.paginate(domain=domain, registrationStatus='REGISTERED'),
        'typeInfos')


def list_activity_types(swf, domain):
    paginator = swf.get_paginator('list_activity_types')
    return iter_collection(
        paginator.paginate(domain=domain, registrationStatus='REGISTERED'),
        'typeInfos')


def poll_for_decision_tasks(swf, domain, task_list, identity):
    paginator = swf.get_paginator('poll_for_decision_task')
    result = None
    for page in paginator.paginate(domain=domain, taskList={'name': task_list},
                                   identity=identity):
        if result is None:
            result = page
        else:
            result['events'].extend(page['events'])
    return result


def poll_for_activity_tasks(swf, domain, task_list, identity):
    return swf.poll_for_activity_task(domain=domain,
                                      taskList={'name': task_list},
                                      identity=identity)


def schedule_activity_task(swf, task, task_list, activity_type, **options):
    attribs = {'activityType': activity_type,
               'activityId': 'activityId-' + str(uuid.uuid4()),
               'taskList': {'name': task_list}}
    attribs.update(options)

    swf.respond_decision_task_completed(
        taskToken=task['taskToken'],
        decisions=[
            {
                'decisionType': 'ScheduleActivityTask',
                'scheduleActivityTaskDecisionAttributes': attribs
            }
        ])


def init_domain(swf):
    """
    :param swf: boto3 SWF client
    :return: domain name for this project
    """
    domain_name = 'SWFSampleDomain'

    # see if domain already exists.  If not, register it.
    for d in list_domains(swf):
        if d['name'] == domain_name:
            break
    else:
        swf.register_domain(name=domain_name,
                            description='{} domain'.format(domain_name),
                            workflowExecutionRetentionPeriodInDays='1')
    return domain_name
