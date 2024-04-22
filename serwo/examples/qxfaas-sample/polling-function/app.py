import json
from qiskit_ibm_runtime import QiskitRuntimeService
from qsserializers import program_serializers, serializers

def lambda_handler(event, context):
    job_id = event["body"]["job_id"]
    qAccesKey = "120ce43e46c5eebcaf34987e3a10aa3e2403fbc4a4f8109be6afb9180258ffba2a0cda741614648e93872f560d4213ffc8c612d19160ff6ed7202d22d81bed28"
    service = QiskitRuntimeService(channel="ibm_quantum", token=qAccesKey)
    try:
        completedJob = service.job(job_id=job_id)
    except Exception as e:
        print(f'Error: {str(e)}')
        completedJob = ''

    if completedJob != '':
        result_data = json.dumps(completedJob.result(), cls=program_serializers.QiskitObjectsEncoder)
        return {
        "statusCode": 200,
        "status": "SUCCEEDED",
        "body": json.dumps(
            {
                "message": result_data,
            }
        ),
    }
    else:
        return event
