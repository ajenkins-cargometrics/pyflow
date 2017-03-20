import logging

import pyflow


def create_workflow():
    """Creates a workflow which pipes a string through 3 lambda functions.
    First the string-upcase function converts the string to uppercase.
    Then the string-reverse function reverses the string, finally the
    string-concat function concatenates a fixed prefix onto the
    reversed string.
    """

    workflow = pyflow.Workflow('StringTransformer', version='1.0', result_task_name='Concat')

    upper_caser_task = workflow.add(pyflow.LambdaTask(
        name='UpperCaser',
        lambda_function_name='string_upcase',
        depends_on=[workflow.start_task],
        num_retries=2,
        retry_interval=5))

    reverser_task = workflow.add(pyflow.LambdaTask(
        name='Reverser',
        lambda_function_name='string_reverse',
        depends_on=[upper_caser_task]))

    workflow.add(pyflow.LambdaTask(
        name='Concat',
        lambda_function_name='string_concat',
        param='Hello',
        depends_on=[reverser_task],
        transform_input=lambda inp: [inp['param'], ' ',
                                     inp['inputs']['Reverser']]))

    return workflow


def main():
    logging.basicConfig()
    pyflow.workflow_runner.logger.setLevel(logging.DEBUG)

    workflow = create_workflow()
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'

    # Will poll indefinitely for decider events
    pyflow.poll_for_executions([workflow], domain=domain, task_list=task_list, identity='string transformer decider')


if __name__ == '__main__':
    main()
