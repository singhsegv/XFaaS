import json
import networkx as nx

from .function import Function

class UserDag:
    # dag configuration (picked up from user file)
    __dag_config_data = dict()

    # map: nodeName -> nodeId (used internally) [NOTE [TK] - This map is changed from nodeName -> NodeId to UserGivenNodeId -> our internal nodeID]
    __nodeIDMap = ({})

    __dag = nx.DiGraph() # networkx directed graph
    __functions = {} # map: functionName -> functionObject

    def __init__(self, user_config_path) -> None:
        try:
            self.__dag_config_data = self.__load_user_spec(user_config_path)
        except Exception as e:
            raise e
        
        for index, node in enumerate(self.__dag_config_data["Nodes"]):
            nodeID = "n" + str(index+1)
            self.__nodeIDMap[node["NodeName"]] = nodeID
            self.__nodeIDMap[node["NodeId"]] = nodeID
            self.__functions[node["NodeName"]] = Function(
                id=node["NodeId"],
                name=node["NodeName"],
                path=node["Path"],
                entry_point=node["EntryPoint"],
                memory=node["MemoryInMB"],
            )

            # TODO: Add support for private cloud related params here in _get_state()
            # TODO: AWS reference also stores ARN and retry, backoff, max attempts etc in __dag
            # generate hardcoded Action's package, etc here?
            self.__dag.add_node(
                nodeID,
                NodeName=node["NodeName"],
                Path=node["Path"],
                EntryPoint=node["EntryPoint"],
                CSP=node.get("CSP"),
                MemoryInMB=node["MemoryInMB"],
            )

        for edge in self.__dag_config_data["Edges"]:
            for key in edge:
                for val in edge[key]:
                    self.__dag.add_edge(self.__nodeIDMap[key], self.__nodeIDMap[val])


    def __load_user_spec(self, user_config_path):
        with open(user_config_path, "r") as user_dag_spec:
            dag_data = json.load(user_dag_spec)

        return dag_data
    
    
    def get_user_dag_name(self):
        return self.__dag_config_data["WorkflowName"]
    

    def get_node_object_map(self):
        return self.__functions
    
    
    def get_node_param_list(self):
        functions_list = []
        for f in self.__functions.values():
            functions_list.append(f.get_as_dict())

        return functions_list
    
