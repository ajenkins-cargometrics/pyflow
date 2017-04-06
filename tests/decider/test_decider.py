import attr
import boto3
import botocore.stub
import pytest

import decision_tasks
import pyflow
from pyflow import workflow_state as ws


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

        # Try invoking an activity
        subscription = swf.invoke_activity('subscribe_topic_activity', 'v1', {'email': 'john.doe@email.com'},
                                           task_list='subscription-activities')

        concatted = swf.invoke_child_workflow('StringConcatter', '1.0', input_arg=[s.result() for s in reversed_strs],
                                              lambda_role=swf.lambda_role).result()

        subscription.result()
        return concatted


class StringConcatter(pyflow.Workflow):
    NAME = 'StringConcatter'
    VERSION = '1.0'

    def run(self, swf, workflow_input):
        return swf.invoke_lambda('string_concat', workflow_input).result()


@pytest.fixture
def s3_client():
    """Returns a mock boto3 S3 client.  See botocore.stub.Stubber docstring for how to use it"""
    return botocore.stub.Stubber(boto3.client('s3'))


@pytest.fixture
def decider(s3_client):
    return pyflow.Decider([StringTransformer(), StringConcatter()],
                          domain='test-domain', task_list='string-transformer-decider',
                          identity='string transformer decider', client=s3_client)


@attr.s
class DeciderTestCase(object):
    name = attr.ib()
    decision_task = attr.ib()
    expected_decisions = attr.ib()


process_decision_task_test_cases = [
    DeciderTestCase('decision_task00', decision_tasks.decision_task00, [
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda1',
             'name': 'string_upcase',
             'input': '"Hello"'}
         },
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda2',
             'name': 'string_upcase',
             'input': '" "'}
         },
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda3',
             'name': 'string_upcase',
             'input': '"World"'}
         },
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda4',
             'name': 'string_upcase',
             'input': '42'}
         }
    ]),

    # Continued from task00, but lambda2 invocation has completed
    DeciderTestCase('decision_task01', decision_tasks.decision_task01, []),

    # Now lambda1 has completed, and lambda4 has failed
    DeciderTestCase('decision_task02', decision_tasks.decision_task02, [
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda5',
             'name': 'string_reverse',
             'input': '"HELLO"'}
         },
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda6',
             'name': 'string_reverse',
             'input': '" "'}
         }
    ]),

    # Now lambda3 has completed
    DeciderTestCase('decision_task03', decision_tasks.decision_task03, [
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda7',
             'name': 'string_reverse',
             'input': '"WORLD"'}
         },
        {'decisionType': 'StartTimer',
         'startTimerDecisionAttributes': {
             'startToFireTimeout': "5",
             'timerId': "sleep1"}
         }
    ]),

    # lambda5 and lambda6 completed
    DeciderTestCase('decision_task04', decision_tasks.decision_task04, []),

    # lambda7 completed
    DeciderTestCase('decision_task05', decision_tasks.decision_task05, []),

    # sleep1 completed
    DeciderTestCase('decision_task06', decision_tasks.decision_task06, [
        {'decisionType': 'ScheduleActivityTask',
         'scheduleActivityTaskDecisionAttributes': {
             'activityId': 'activity1',
             'activityType': {'name': 'subscribe_topic_activity', 'version': 'v1'},
             'input': '{"email": "john.doe@email.com"}',
             'taskList': {'name': 'subscription-activities'}}
         },
        {'decisionType': 'StartChildWorkflowExecution',
         'startChildWorkflowExecutionDecisionAttributes': {
             'workflowId': 'child_workflow1',
             'workflowType': {'name': 'StringConcatter', 'version': '1.0'},
             'input': '["OLLEH", " ", "DLROW"]',
             'lambdaRole': 'arn:aws:iam::528461152743:role/swf-lambda'}
         }
    ]),

    # activity1 completed
    DeciderTestCase('decision_task07', decision_tasks.decision_task07, []),

    # This is the first decision task for the StringConcatter child workflow
    DeciderTestCase('decision_task08', decision_tasks.decision_task08, [
        {'decisionType': 'ScheduleLambdaFunction',
         'scheduleLambdaFunctionDecisionAttributes': {
             'id': 'lambda1',
             'name': 'string_concat',
             'input': '["OLLEH", " ", "DLROW"]'}
         }
    ]),

    # In parent, notifies that child workflow started
    DeciderTestCase('decision_task09', decision_tasks.decision_task09, []),

    # lambda1 in the child workflow completed
    DeciderTestCase('decision_task10', decision_tasks.decision_task10, [
        {'decisionType': 'CompleteWorkflowExecution',
         'completeWorkflowExecutionDecisionAttributes': {
             'result': '"OLLEH DLROW"'}
         }
    ]),

    # child_workflow1 completed
    DeciderTestCase('decision_task11', decision_tasks.decision_task11, [
        {'decisionType': 'CompleteWorkflowExecution',
         'completeWorkflowExecutionDecisionAttributes': {
             'result': '"OLLEH DLROW"'}
         }
    ])
]


@pytest.mark.parametrize('test_case', process_decision_task_test_cases,
                         ids=[tc.name for tc in process_decision_task_test_cases])
def test_process_decision_task(decider, test_case):
    decision_helper = decider.process_decision_task(test_case.decision_task)
    assert test_case.expected_decisions == decision_helper.decisions


def test_process_decision_maker_cumulative(decider):
    """Like test_process_decision_task, but process all decision tasks in one decider instance"""
    for test_case in process_decision_task_test_cases:
        decision_helper = decider.process_decision_task(test_case.decision_task)
        assert test_case.expected_decisions == decision_helper.decisions, test_case.name


def test_process_decision_task_with_workflow_failure(decider):
    decision_helper = decider.process_decision_task(decision_tasks.decision_task02_error)

    assert len(decision_helper.decisions) == 1
    decision = decision_helper.decisions[0]
    assert decision['decisionType'] == 'FailWorkflowExecution'
    assert decision['failWorkflowExecutionDecisionAttributes']['reason']
    assert decision['failWorkflowExecutionDecisionAttributes']['details']


def test_invalid_workflow_input(decider):
    """Validate correct response to invalid JSON being passed as workflow input"""

    # The workflow should fail, but the decider itself shouldn't fail.
    decision_helper = decider.process_decision_task(decision_tasks.invalid_workflow_input)

    assert decision_helper.decisions[0]['decisionType'] == 'FailWorkflowExecution'


def test_invalid_activity_output(decider):
    """Validate correct response to an activity task returning invalid JSON string"""

    # The correct behavior is for the task to fail, but decider and workflow to continue
    decision_helper = decider.process_decision_task(decision_tasks.invalid_activity_output)

    assert decision_helper.workflow_state.invocation_states['activity1'].state == ws.InvocationState.FAILED


def test_invalid_child_workflow_output(decider):
    "Validate correct response to a child workflow returning invalid JSON string"

    decision_helper = decider.process_decision_task(decision_tasks.invalid_child_workflow_output)

    assert decision_helper.workflow_state.invocation_states['child_workflow1'].state == ws.InvocationState.FAILED
