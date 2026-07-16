FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY *.py ${LAMBDA_TASK_ROOT}/
COPY transforms ${LAMBDA_TASK_ROOT}/transforms

CMD ["lambda_function.lambda_handler"]
