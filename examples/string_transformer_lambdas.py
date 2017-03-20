"""
Lambda functions for the string_transformer_workflow example workflow.

To deploy:

  zip /tmp/string-transformers.zip string_transformer_lambdas.py

  for name in string_upcase string_reverse string_concat; do
    aws lambda create-function --function-name $name --runtime python2.7 \
      --role $LAMBDA_ROLE --handler string_transformer_lambdas.${name} \
      --zip-file fileb:///tmp/string-transformers.zip --publish
  done

To delete the functions:

  for name in string_upcase string_reverse string_concat; do
    aws lambda delete-function --function-name $name
  done

"""


def string_upcase(event, context):
    return event.upper()


def string_reverse(event, context):
    return ''.join(reversed(list(event)))


def string_concat(event, context):
    return ''.join(event)
