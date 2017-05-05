import logging

import pyflow


class StringTransformer(pyflow.Workflow):
    NAME = 'StringTransformer'
    VERSION = '1.1'
    OPTIONS = {'defaultLambdaRole': 'arn:aws:iam::528461152743:role/swf-lambda',
               'defaultTaskStartToCloseTimeout': 60}

    # Descriptors provide a way to declare shortcuts for remote invocations
    string_upcase = pyflow.LambdaDescriptor('string_upcase')
    string_concat = pyflow.ChildWorkflowDescriptor('StringConcatter', '1.2')

    def run(self, workflow_input):
        # for loops work.  In this case upcased will contain a list of futures
        upcased = []
        for s in workflow_input:
            upcased.append(self.string_upcase(s))

        # demonstrate error handling
        try:
            # pass a number where a string is expected
            self.swf.invoke_lambda('string_upcase', 42).result()
            assert False, "Shouldn't get to here"
        except pyflow.InvocationFailedException:
            pass

        # list comprehensions as well
        reversed_strs = [self.swf.invoke_lambda('string_reverse', s.result()) for s in upcased]

        # Sleep for 5 seconds
        self.swf.sleep(5)

        timer = self.swf.start_timer(5)

        # wait with timeout, intentionally triggering a timeout.
        try:
            self.swf.timed_wait_for_all(1, timer, reversed_strs)
        except pyflow.WaitTimedOutException:
            pass
        else:
            assert False, "Timeout didn't happen"

        # Wait for all futures to finish before proceeding
        self.swf.wait_for_all(reversed_strs)

        concatted = self.string_concat([s.result() for s in reversed_strs])

        return concatted.result()


class StringConcatter(pyflow.Workflow):
    NAME = 'StringConcatter'
    VERSION = '1.2'
    OPTIONS = {'defaultLambdaRole': 'arn:aws:iam::528461152743:role/swf-lambda'}

    def run(self, workflow_input):
        return self.swf.invoke_lambda('string_concat', workflow_input).result()


def main():
    logging.basicConfig()
    pyflow.logger.setLevel(logging.DEBUG)

    workflows = [StringTransformer, StringConcatter]
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'

    # Will poll indefinitely for decider events
    pyflow.poll_for_executions(workflows, domain=domain, task_list=task_list, identity='string transformer decider')


if __name__ == '__main__':
    main()
