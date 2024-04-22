import json
import numpy as np
from qiskit import QuantumCircuit
from qiskit.circuit.library import EfficientSU2
from qiskit.quantum_info import PauliList
# from circuit_knitting.cutting import partition_problem, generate_cutting_experiments
from qsserializers import program_serializers, serializers
# from qiskit_ibm_runtime import QiskitRuntimeService, Sampler, Session, Options
from qiskit_braket_provider import AWSBraketProvider


def objectify(data):
  jsonData = json.loads(data['serialized_subckts '])
  print("json data here", jsonData)
  serializedSubExps = jsonData['sub-experiments']
  numExp = len(serializedSubExps)
  listSC = []
  for i in range(0, numExp):
    v = serializedSubExps[i]
    lc = []
    for cstr in v['sub-circuits']:
      sub_circuit = serializers.circuit_deserializer(cstr)
      lc.append(sub_circuit)
    listSC.append(lc)
  return listSC

# def lambda_handler(event, context):
#     # with open("result.json") as fp:
#     #     event = json.load(fp)
#     subexp_list = objectify(event["body"])
#     qAccesKey = "120ce43e46c5eebcaf34987e3a10aa3e2403fbc4a4f8109be6afb9180258ffba2a0cda741614648e93872f560d4213ffc8c612d19160ff6ed7202d22d81bed28"
#     backendName = 'ibmq_qasm_simulator'
#     service = QiskitRuntimeService(channel="ibm_quantum", token=qAccesKey)
#     session = Session(service=service, backend='ibmq_qasm_simulator')

#     # run just a single subexperiment
#     sampler1 = Sampler(session=session)
#     job1 = sampler1.run(subexp_list[0])
#     job_id = job1.job_id()

#     retval = {
#         "statusCode": 200,
#         "body": {
#                 "job_id": job_id,
#             }
#     }

#     print("return value", retval)
#     return retval

def lambda_handler(event, context):
    provider = AWSBraketProvider()
    ionq_device = provider.get_backend("IonQ Device")
    circuit = QuantumCircuit(3)
    circuit.h(0)
    for qubit in range(1, 3):
        circuit.cx(0, qubit)
    ionq_task = ionq_device.run(circuit, shots=100)
    print("task result here", ionq_task.result())


if __name__ == "__main__":
    lambda_handler({}, {})
