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

        # list comprehensions as well
        reversed_strs = [swf.invoke_lambda('string_reverse', s.result()) for s in upcased]

        # Sleep for 5 seconds
        swf.sleep(5)

        # Wait for all futures to finish before proceeding.  This normally isn't necessary since just calling result()
        # on each future would accomplish the same thing.
        swf.wait_for_all(reversed_strs)

        concatted = swf.invoke_lambda('string_concat', [s.result() for s in reversed_strs])

        return concatted.result()


def main():
    logging.basicConfig()
    pyflow.decider.logger.setLevel(logging.DEBUG)

    workflow = StringTransformer()
    domain = 'SWFSampleDomain'
    task_list = 'string-transformer-decider'

    # Will poll indefinitely for decider events
    pyflow.poll_for_executions([workflow], domain=domain, task_list=task_list, identity='string transformer decider')


if __name__ == '__main__':
    main()
