# FROM seshapad/aws-lambda-qiskit:base
FROM seshapad/qxfaas-qiskit-1.0:base
COPY standalone_Reconstruction_runner.py requirements.txt ./
COPY python ./
RUN pip install -r requirements.txt
# Command can be overwritten by providing a different command in the template directly.

CMD ["standalone_Reconstruction_runner.lambda_handler"]
