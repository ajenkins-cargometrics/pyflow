import logging

import pyflow


class StringTransformer(pyflow.Workflow):
    NAME = 'StringTransformer'
    VERSION = '1.0'

    def run(self, swf, workflow_input):
        upcased = [swf.invoke_lambda('string_upcase', s) for s in workflow_input]

        reversed_strs = [swf.invoke_lambda('string_reverse', s.result()) for s in upcased]

        swf.sleep(5)

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
