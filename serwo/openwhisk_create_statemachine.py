"""
Remarks:
* Each cloud could use its own folder with an API contract
"""
import os
import shutil
import zipfile
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from python.src.utils.classes.openwhisk.user_dag import UserDag
from python.src.utils.classes.commons.logger import LoggerFactory

logger = LoggerFactory.get_logger(__file__, log_level="INFO")


class OpenWhisk:
    def __init__(self, user_dir, dag_file_name, part_id) -> None:
        self.__user_dir = Path(user_dir)
        self.__dag_file_name = dag_file_name
        self.__part_id = part_id

        self.__parent_directory_path = Path(__file__).parent

        # xfaas specfic directories
        self.__serwo_build_dir = self.__user_dir / "build" / "workflow"
        self.__serwo_resources_dir = self.__serwo_build_dir / "resources"
        self.__serwo_utils_dir = self.__parent_directory_path / "python"

        # TODO: Change me to generic private cloud implementation
        self.__runner_template_dir = self.__serwo_utils_dir / "src" / "runner-templates" / "openwhisk"

        self.__dag_definition_path = self.__user_dir / self.__dag_file_name

        # openwhisk specific directories
        # TODO: Decide between wsk cli and REST api based implementation
        self.__openwhisk_build_dir = self.__serwo_build_dir / "openwhisk"
        self.__openwhisk_functions_dir = self.__openwhisk_build_dir / "functions"
        self.__openwhisk_artifacts_dir = self.__openwhisk_build_dir / "artifacts"

        # DAG related parameters
        self.__user_dag = UserDag(self.__dag_definition_path)


    def __create_environment(self):
        """
        TODO: Check if the generated folders need to have __init__.py (be a module)
        """
        self.__openwhisk_functions_dir.mkdir(parents=True, exist_ok=True)
        self.__openwhisk_artifacts_dir.mkdir(parents=True, exist_ok=True)
    

    def __render_runner_template(self, runner_template_dir, function_id, function_name, function_runner_filename):
        """
        """
        runner_template_filename = function_runner_filename
        
        try:
            file_loader = FileSystemLoader(runner_template_dir)
            env = Environment(loader=file_loader)
            template = env.get_template("runner_template.py")
            logger.info(
                f"Created jinja2 environement for templating Openwhisk function ids for function::{function_name}"
            )
        except:
            raise Exception(
                f"Unable to load environment for Openwhisk function id templating for function::{function_name}"
            )
        
        # render the template
        try:
            output = template.render(function_id_placeholder=function_id)
            with open(f"{runner_template_dir}/{runner_template_filename}", "w") as out:
                out.write(output)
                logger.info(
                    f"Rendered and flushed the runner template for Openwhisk function for function::{function_name}"
                )
        except Exception as e:
            logger.error(e)
            raise Exception(
                f"Unable to render the function runner template for Openwhisk function for function::{function_name}"
            )

        return runner_template_filename
    

    def __append_xfaas_default_requirements(self, filepath):
        """
        Appends XFaaS dependencies to the function requirements
        """
        with open(filepath, "r") as file:
            lines = file.readlines()
            lines.append("psutil\n")
            lines.append("objsize\n")
            unqiue_dependencies = set(lines)
            file.flush()
            
        with open(filepath, "w") as file:
            for line in [x.strip("\n") for x in sorted(unqiue_dependencies)]:
                file.write(f"{line}\n")
    

    def __create_openwhisk_actions(
        self, 
        user_fn_path,
        fn_name,
        fn_module_name,
        runner_template_filename,
        runner_template_dir,
        runner_filename
    ):
        """
        TODO: Check WTF am I doing
        user_fn_path: Workflow code is taken from hardcoded XFaaS samples from this path 
        """
        fn_requirements_filename = "requirements.txt"
        src_fn_dir = user_fn_path / fn_requirements_filename
        dst_fn_dir = self.__openwhisk_functions_dir / fn_name

        dst_requirements_path = dst_fn_dir / fn_requirements_filename
        
        logger.info(f"Creating function directory for {fn_name}")
        if not os.path.exists(dst_fn_dir):
            os.makedirs(dst_fn_dir)

        logger.info(f"Moving requirements file for {fn_name} for user at to {dst_fn_dir}")
        shutil.copyfile(src=src_fn_dir, dst=dst_requirements_path)

        logger.info(f"Adding default requirements {fn_name}")
        self.__append_xfaas_default_requirements(dst_requirements_path)

        # place the dependencies folder from the user function path if it exists
        if os.path.exists(user_fn_path / "dependencies"):
            shutil.copytree(
                user_fn_path / "dependencies", 
                dst_fn_dir / "dependencies", 
                dirs_exist_ok=True
            )

        logger.info(f"Moving xfaas boilerplate for {fn_name}")
        shutil.copytree(src=self.__serwo_utils_dir, dst=dst_fn_dir / "python", dirs_exist_ok=True)

        logger.info(f"Generating Runners for function {fn_name}")

        fnr_string = f"USER_FUNCTION_PLACEHOLDER"
        temp_runner_path = user_fn_path / f"{fn_name}_temp_runner.py"
        runner_template_path = runner_template_dir / runner_template_filename
        
        print("Here", runner_template_path)
        with open(runner_template_path, "r") as file:
            contents = file.read()
            contents = contents.replace(fnr_string, fn_module_name)

        with open(temp_runner_path, "w") as file:
            file.write(contents)

        # TODO - Fix the stickytape issue: GitHub Issue link - https://github.com/dream-lab/XFaaS/issues/4
        logger.info(f"Stickytape the runner template for dependency resolution")
        runner_file_path = dst_fn_dir / f"{runner_filename}.py"
        os.system(f"stickytape {temp_runner_path} > {runner_file_path}")

        logger.info(f"Deleting temporary runner")
        os.remove(temp_runner_path)

        logger.info(f"Successfully created build directory for function {fn_name}")


    def __create_standalone_runners(self):
        """
        TODO: Create functions with __main__.py name
        """
        function_metadata_list = self.__user_dag.get_node_param_list()
        function_object_map = self.__user_dag.get_node_object_map()
        
        for function_metadata in function_metadata_list:
            function_name = function_metadata["name"]
            function_runner_filename = function_object_map[function_name].get_runner_filename()
            
            # generalize later
            function_runner_filename = "__main__"
            
            function_path = function_object_map[function_name].get_path()
            function_module_name = function_object_map[function_name].get_module_name()
            function_id = function_object_map[function_name].get_id()

            # template the function runner template in the runner template directory
            runner_template_filename = self.__render_runner_template(
                runner_template_dir=self.__runner_template_dir,
                function_id=function_id,
                function_name=function_name,
                function_runner_filename=function_runner_filename
            )

            logger.info(f"Starting Standalone Runner Creation for function {function_name}")

            self.__create_openwhisk_actions(
                self.__parent_directory_path / function_path,
                function_name,
                function_module_name,
                function_runner_filename,
                self.__runner_template_dir,
                runner_template_filename
            )

            runner_template_filepath = self.__runner_template_dir / runner_template_filename
            logger.info(f"Deleting Temporary Runner Template at {runner_template_filepath}")
            os.remove(f"{runner_template_filepath}")


    def __create_workflow_orchestrator(self, file_name_prefix):
        """
        Create composer.js file for openwhisk composer
        TODO: Fix me -> Hardcoded AF js file
        TODO: Setup a local npm + node combo with openwhisk-composer library
        """
        composer_file_path = self.__openwhisk_build_dir / f"{file_name_prefix}_workflow_composition.js"

        # TODO: Everything here is dummy stuff
        # Another assumption here is that action creation will be taken care of at some other place
        action_sequence = []
        for func_name in self.__user_dag.get_node_object_map():
            action_name = f"/guest/{self.__user_dag.get_user_dag_name()}/{func_name}"
            action_sequence.append(f'composer.action("{action_name}")')
        
        with open(composer_file_path, "w") as f:
            f.write('const composer = require("openwhisk-composer");\n\n')

            temp = ", ".join(action_sequence)
            f.write(f'module.exports = composer.sequence({temp});\n')

        # TODO: This is also temporary
        # To allow parallel workflows and other stuff, a redis instance is needed
        # with an input.json file with corresponding redis info
        redis_input_file_path = self.__openwhisk_build_dir / f"{file_name_prefix}_input.json"
        with open(redis_input_file_path, "w") as f:
            f.write("""
{
    "$composer": {
        "redis": {
            "uri": "redis://owdev-redis.openwhisk.svc.cluster.local:6379"
        },
        "openwhisk": {
            "ignore_certs": true
        }
    }
}
""")
    

    def build_resources(self):
        """
        TODO: Implement me
        1. Create action compatible function files
        2. Create openwhisk composer files -> Check AWS logic for the graph creation
        """
        logger.info(f"Creating environment for {self.__user_dag.get_user_dag_name()}")
        self.__create_environment()

        logger.info(f"Initating standalone runner creation for {self.__user_dag.get_user_dag_name()}")
        self.__create_standalone_runners()
        
        logger.info(f"Initating openwhisk composer js orchestrator for {self.__user_dag.get_user_dag_name()}")
        self.__create_workflow_orchestrator(self.__user_dag.get_user_dag_name())
        
        # Some API gateway??


    def build_workflow(self):
        """
        TODO: Implement me
        TODO: Differentiate between docker actions vs pure python actions
        1. Create zip artifacts for all the functions
        2. Create json file out of the compose.js file

        Need wsk, node, npm
        """
        
        # Creates a zip artifact from each function
        for func_name in self.__user_dag.get_node_object_map():
            # func = self.__user_dag.get_node_object_map()[func_name]
            
            py_file = self.__openwhisk_functions_dir / func_name / "__main__.py"
            req_file = self.__openwhisk_functions_dir / func_name / "requirements.txt"
            
            output_artifact_path = self.__openwhisk_build_dir / "artifacts" / f"{func_name}.zip"

            # Zip the requirements and __main__.py from input_dir -> Write to output_dir
            with zipfile.ZipFile(output_artifact_path, "w") as f:
                f.write(py_file, os.path.basename(py_file))
                f.write(req_file, os.path.basename(req_file))

        composer_input_path = self.__openwhisk_build_dir / f"{self.__user_dag.get_user_dag_name()}_workflow_composition.js"
        composer_output_path = self.__openwhisk_build_dir / f"{self.__user_dag.get_user_dag_name()}_workflow_composition.json"

        # TODO: Change me... Be a better human than this
        os.system(f"source ~/.bashrc && compose.js {composer_input_path} -o {composer_output_path}")


    def deploy_workflow(self):
        """
        TODO: Setup wsk if not present somehow
        1. Deploy all the actions
        2. Deploy composition
        3. Web hooks and stuff
        # Create or update the action

        TODO: wsk -i action invoke /guest/graph-workflow -P input.json --result -> Handle this input.json thingy somehow
        TODO: Throw Exception if wsk command is not working -> Or find a better way to connect to the cluster
        """
        logger.info(f"Deleting any existing package with same name")

        # TODO: Check if deleting a package deletes all the actions automatically?
        os.system(f"wsk -i package delete {self.__user_dag.get_user_dag_name()}")
        os.system(f"wsk -i package create {self.__user_dag.get_user_dag_name()}")

        # Create actions manually
        for func_name in self.__user_dag.get_node_object_map():
            action_name = f"/guest/{self.__user_dag.get_user_dag_name()}/{func_name}"
            action_zip_path = self.__openwhisk_artifacts_dir / func_name / ".zip"
            os.system(f"wsk -i action create {action_name} --kind python:3 {action_zip_path} --timeout 300000 --concurrency 10")

        # Create composition
        composition_name = f"{self.__user_dag.get_user_dag_name()}-composition"
        composer_config_path = self.__openwhisk_build_dir / f"{self.__user_dag.get_user_dag_name()}_workflow_composition.json"
        os.system(f"source ~/.bashrc && deploy.js {composition_name} {composer_config_path} -w -i")
