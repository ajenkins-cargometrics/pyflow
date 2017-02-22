import pyflow


def create_workflow():
    """Creates a workflow which pipes a string through 3 lambda functions.
    First the string-upcase function converts the string to uppercase.
    Then the string-reverse function reverses the string, finally the
    string-concat function concatenates a fixed prefix onto the
    reversed string.
    """
    
    workflow = pyflow.Workflow('StringTransformer', version='1.0')

    upper_caser_task = workflow.add(pyflow.LambdaTask(
        name='UpperCaser',
        lambda_function='string-upcase',
        depends_on=[workflow.start_task]))

    reverser_task = workflow.add(pyflow.LambdaTask(
        name='Reverser',
        lambda_function='string-reverse',
        depends_on=[upper_caser_task]))

    sleep_task = workflow.add(pyflow.TimerTask(
        name='Sleep',
        duration=30,
        depends_on=[reverser_task]))

    workflow.add(pyflow.LambdaTask(
        name='Concat',
        lambda_function='string-concat',
        param='Hello',
        depends_on=[sleep_task, reverser_task],
        transform_input=lambda inp: [inp['param'], ' ', inp['inputs']['Reverser']]))

    return workflow


def main():
    workflow = create_workflow()
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'
    pyflow.ensure_workflow_registered(
        workflow,
        domain='SWFSampleDomain')

    # Will poll indefinitely for decider events
    pyflow.poll_for_executions(workflow, domain=domain, task_list=task_list)


if __name__ == '__main__':
    main()
