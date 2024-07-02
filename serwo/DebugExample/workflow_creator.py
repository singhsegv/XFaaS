import os
import glob
import zipfile

def create_artifact():
    output_artifact_path = os.path.join(".", "artifacts", "Func.zip")

    files_to_zip = []
    for file in os.listdir(os.path.join(".", "Func")):
        if file.endswith(".py"):
            curr_file_path = os.path.abspath(os.path.join(os.path.abspath("."), "Func", file))
            files_to_zip.append([curr_file_path, os.path.basename(curr_file_path)])
    
    with zipfile.ZipFile(output_artifact_path, "w") as archive:
        for fz in files_to_zip:
            archive.write(filename=fz[0], arcname=os.path.basename(fz[1]))

        req_file_path = os.path.join(".", "Func", "requirements.txt")
        archive.write(filename=req_file_path, arcname=os.path.basename(req_file_path))

        xfaas_folder_path = os.path.abspath(os.path.join(".", "Func", "python", "**", "*"))
        for file in glob.glob(xfaas_folder_path, recursive=True):
            path = file.split("python/")[1]
            archive.write(filename=file, arcname=os.path.join("python", path))

def create_actions(lim):
    for i in range(lim):
        try:
            os.system(f"wsk -i action delete /guest/debug/node_{i}")
        except Exception as e:
            # TODO: Handle me gracefully
            print("Either the openwhisk action does not exist or some error happened")
            print(e)  
        
    try:
        os.system(f"wsk -i action delete /guest/debug/orchestrator")
    except Exception as e:
        # TODO: Handle me gracefully
        print("Either the openwhisk workflow orchestrator does not exist or some error happened")
        print(e)
    
    try:
        os.system(f"wsk -i package delete debug")
    except Exception as e:
        print("Package deletion failed")
        print(e)

    try:
        os.system(f"wsk -i package create debug")
    except Exception as e:
        print("Package creation failed")
        print(e)

    for i in range(lim):
        action_name = f"/guest/debug/node_{i}"
        action_zip_path = os.path.abspath(os.path.join(".", "artifacts", "Func.zip"))
        
        os.system(f"wsk -i action create {action_name} --kind python:3 {action_zip_path}")
        # workflow_update_cmd = f"wsk -i action update {action_name} --timeout 300000 --concurrency 10"
        # workflow_update_cmd += " --param '$composer' '{"
        # workflow_update_cmd += '"redis":{"uri":{"url":"'
        # workflow_update_cmd += "redis://owdev-redis.openwhisk.svc.cluster.local:6379"
        # workflow_update_cmd += '"}'
        # workflow_update_cmd += '},"openwhisk":{"ignore_certs":'
        # workflow_update_cmd += "true"
        # workflow_update_cmd += '}'
        # workflow_update_cmd += "}'"

        # try:
        #     os.system(workflow_update_cmd)
        # except Exception as e:
        #     print("*" * 30)
        #     print("Error in updating action")
        #     print(e)
        #     print("*" * 30)

def main():
    create_artifact()
    create_actions(5)

main()