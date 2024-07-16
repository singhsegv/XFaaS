FROM public.ecr.aws/lambda/python:3.10
COPY . .
RUN pip install -r requirements.txt
# Command can be overwritten by providing a different command in the template directly.
CMD ["standalone_app_runner.lambda_handler"]