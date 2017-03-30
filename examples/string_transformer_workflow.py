import logging

import pyflow


class StringTransformer(pyflow.Workflow):
    NAME = 'StringTransformer'
    VERSION = '1.0'

    def run(self, swf, workflow_input):
        # for loops work.  In this case upcased will contain a list of futures
        upcased = []
        for s in workflow_input:
            upcased.append(swf.invoke_lambda('string_upcase', s))

        # demonstrate error handling
        try:
            # pass a number where a string is expected
            swf.invoke_lambda('string_upcase', 42).result()
            assert False, "Shouldn't get to here"
        except pyflow.InvocationFailedException:
            pass

        # list comprehensions as well
        reversed_strs = [swf.invoke_lambda('string_reverse', s.result()) for s in upcased]

        # Sleep for 5 seconds
        swf.sleep(5)

        # Wait for all futures to finish before proceeding.  This normally isn't necessary since just calling result()
        # on each future would accomplish the same thing.
        swf.wait_for_all(reversed_strs)

        subscribed = swf.invoke_activity('subscribe_topic_activity', 'v1', {'email': 'john.doe@email.com'},
                                         task_list='subscription-activities')
        print "Subscription info: {}".format(subscribed.result())

        concatted = swf.invoke_child_workflow('StringConcatter', '1.0', input_arg=[s.result() for s in reversed_strs],
                                              lambda_role=swf.lambda_role)

        return concatted.result()


class StringConcatter(pyflow.Workflow):
    NAME = 'StringConcatter'
    VERSION = '1.0'

    def run(self, swf, workflow_input):
        return swf.invoke_lambda('string_concat', workflow_input).result()


def main():
    logging.basicConfig()
    pyflow.decider.logger.setLevel(logging.DEBUG)

    workflows = [StringTransformer(), StringConcatter()]
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'

    # Will poll indefinitely for decider events
    pyflow.poll_for_executions(workflows, domain=domain, task_list=task_list, identity='string transformer decider')


if __name__ == '__main__':
    main()
