import json
import numpy as np
from numpy.lib.shape_base import kron
from qiskit.circuit.library import EfficientSU2
from qiskit.quantum_info import PauliList
from circuit_knitting.cutting import partition_problem, generate_cutting_experiments
from qsserializers import program_serializers, serializers
from qiskit_ibm_runtime import QiskitRuntimeService


def jsonifyCuts(subexperiments):
  jsonDict = {}
  l = []
  i = 0
  # numExp = len(subexperiments)
  for key in subexperiments.keys():
    if isinstance(subexperiments[key], list):
      lc = []
      for sc in subexperiments[key]:
        scStr = serializers.circuit_serializer(sc)
        lc.append(scStr)
    jsonStr = {'sub-circuits': lc, 'id': key}
    l.append(jsonStr)
  jsonDict['sub-experiments'] = l
  return json.dumps(jsonDict)


def lambda_handler(event, context):
    qc = EfficientSU2(4, entanglement="linear", reps=2).decompose()
    qc.assign_parameters([0.4] * len(qc.parameters), inplace=True)
    observables = PauliList(["ZZII", "IZZI", "IIZZ", "XIXI", "ZIZZ", "IXIX"])
    partitioned_problem = partition_problem(
        circuit=qc, partition_labels="AABB", observables=observables
    )
    subcircuits = partitioned_problem.subcircuits
    print("printing subcircuits here", subcircuits)
    subobservables = partitioned_problem.subobservables
    bases = partitioned_problem.bases
    subexperiments, coefficients = generate_cutting_experiments(
      circuits=subcircuits,
      observables=subobservables,
      num_samples=np.inf
    )
    print("here", subexperiments)
    serialized_subckts = jsonifyCuts(subexperiments=subexperiments)
    retval = {
        "statusCode": 200,
        "body": {
            "serialized_subckts": serialized_subckts
        }
    }
    print("return val", retval)
    return retval
