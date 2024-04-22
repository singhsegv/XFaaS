from python.src.utils.classes.commons.serwo_objects import SerWOObject
import logging
import json
from qiskit_ibm_runtime import QiskitRuntimeService, Sampler, Options, Session,RuntimeJob
from .qsserializers import  serializers
import time
# import sys
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# def est_exec_curr_time(job):
#     job_metrics=job.metrics()
#     print(job_metrics)
#     created_time=datetime.strptime(job_metrics['timestamps']['created'], '%Y-%m-%dT%H:%M:%S.%fZ')
#     est_com_time=datetime.strptime(job_metrics['estimated_completion_time'], '%Y-%m-%dT%H:%M:%S.%fZ') 
#     print(job.metrics())
#     diff=est_com_time-created_time
#     print(diff)
#     check_time=datetime.now()+diff
#     return check_time

def experiment_list_destrilizer(dict):
  n_dict={}
  for key in dict:
    list=[]
    exp_list =dict[key]
    for subex in exp_list:
      list.append(serializers.circuit_deserializer(subex))
      n_dict[key]=list
  return n_dict   

def user_function(xfaas_object) -> SerWOObject:
  try:
    logging.info("We are in the fn")
    body=xfaas_object.get_body()
    
    subexperiments_trans=experiment_list_destrilizer(body["Transpiled_Subexperiment"])
    del(body["Transpiled_Subexperiment"])
    qAccesKey = "120ce43e46c5eebcaf34987e3a10aa3e2403fbc4a4f8109be6afb9180258ffba2a0cda741614648e93872f560d4213ffc8c612d19160ff6ed7202d22d81bed28"
    # backendName = 'ibmq_qasm_simulator'
    service = QiskitRuntimeService(channel="ibm_quantum", token=qAccesKey)
    # service = QiskitRuntimeService()    
    backend = service.get_backend('ibmq_qasm_simulator')
    logging.info("== submission start ==")
    options = Options()
    options.execution.shots = 4000
    options.transpilation.skip_transpilation = True
    options.resilience_level = 0 # set to 1 if you need measurement error mitigation
    session = Session(backend=backend)
    sampler = Sampler(session=session,options=options)
    results ={}
    li=[]
    for label, subexp in subexperiments_trans.items():
      job = sampler.run(subexp)
      results[label]= job.job_id()
      
      # time.sleep(10)
      # wait_time=est_exec_curr_time(job)
      # print(job.job_id," Wait time: ",wait_time)
      # time_list.append(check_time)
    logging.info("== submission end ==")
    # print(time_list)
    # print("Maximum timestamp",max(time_list))
    # logging.info("Max Execution Time info:"+max(time_list))
    # body["Sleep_timeout"]=max(time_list)
    
    body["message"]="Submission completed sucessfully"
    body["submit_job_ids"]=results
    return SerWOObject(body=body)
  except Exception as e:
    logging.info(e)
    logging.info(e)
    logging.info("Error in Submit function")
    raise Exception("[SerWOLite-Error]::Error at user function",e)
  
# f=open("/home/tarun/XFaaS/serwo/examples/NewQuantumWorkflow/transpiler_out.json")
# body=json.load(f)
# body=json.loads(body)
# z=user_function(SerWOObject(body=body))
# body=z.get_body()
# obj=json.dumps(body,default=str)
# with open("/home/tarun/XFaaS/serwo/examples/NewQuantumWorkflow/submit_out.json", "w") as f:
#   json.dump(obj, f)
# logging.info("Output object:"+str(body))