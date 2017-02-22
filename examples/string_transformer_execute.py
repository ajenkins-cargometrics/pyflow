import pyflow


def main():
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'
    workflow_name = 'StringTransformer'
    workflow_version = '1.0'
    lambda_role='arn:aws:iam::528461152743:role/swf-lambda'

    workflow_id = pyflow.start_workflow(
        domain=domain,
        workflow_name=workflow_name,
        workflow_version=workflow_version,
        task_list=task_list,
        lambda_role=lambda_role,
        input='World')

    print "Workflow started with workflow_id {}".format(workflow_id)


if __name__ == '__main__':
    main()
