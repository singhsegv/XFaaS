import json
import copy
import random
import string
import networkx as nx
from collections import defaultdict

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
            # nodeID = "n" + node["NodeName"] # Uncomment to make debugging easier

            self.__nodeIDMap[node["NodeName"]] = nodeID
            self.__nodeIDMap[node["NodeId"]] = nodeID
            self.__functions[node["NodeName"]] = Function(
                id=node["NodeId"],
                name=node["NodeName"],
                path=node["Path"],
                entry_point=node["EntryPoint"],
                memory=node["MemoryInMB"],
            )

            workflowName = self.__dag_config_data["WorkflowName"]
            nodeName = node["NodeName"]

            self.__dag.add_node(
                nodeID,
                NodeName=node["NodeName"],
                Path=node["Path"],
                EntryPoint=node["EntryPoint"],
                CSP=node.get("CSP"),
                MemoryInMB=node["MemoryInMB"],
                machine_list=[nodeID],
                pre="",
                ret=[f'composer.action("/guest/{workflowName}/{nodeName}")'],
                var=self._generate_random_variable_name(),
            )

        for edge in self.__dag_config_data["Edges"]:
            for key in edge:
                for val in edge[key]:
                    self.__dag.add_edge(self.__nodeIDMap[key], self.__nodeIDMap[val])

    def _generate_random_variable_name(self, n=4):
        res = ''.join(random.choices(string.ascii_letters, k=n))
        return str(res).lower()
    
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
    
    def _get_orchestrator_code_linear_merge(self, graph: nx.DiGraph, nodes):
        """
        TODO: Complete me
        """
        pre = ""
        last = nodes[-1]
        previous_var = None
        
        return "", "", ""
    
    def _get_orchestrator_code_parallel_merge(self, graph: nx.DiGraph, nodes):
        """
        TODO: Complete me
        """
        pre = ""
        last = nodes[-1]
        previous_var = None

        return "", "", ""
    
    def _merge_linear_nodes(self, graph: nx.DiGraph, node_list: list):
        """
        Doesn't return anything since operating on deep copy of graphs might get heavy.
        Works on side effect on nx.Digraph.
        """
        if len(node_list) == 0:
            return
        
        new_node_machine_list = []
        for node in node_list:
            new_node_machine_list.extend(graph.nodes[node]["machine_list"])

        new_node_id = "n" + str(node_list)
        pre, ret, var = self._get_orchestrator_code_linear_merge(graph, node_list)
        graph.add_node(new_node_id,  pre=pre, ret=ret, var=var, machine_list=new_node_machine_list)

        for u, v in list(graph.edges()):
            if v == node_list[0]:
                # replace the first node of the sequence with sequence node's id
                graph.add_edge(u, new_node_id)
            
            if u == node_list[-1]:
                # make the last node of the sequence point to sequence's next node (where sequence ends)
                graph.add_edge(new_node_id, v)
        
        # remove the individual nodes
        for n in node_list:
            graph.remove_node(n)

        return

    
    def _collapse_linear_chains(self, graph: nx.DiGraph):
        """
        Doesn't return anything since operating on deep copy of graphs might get heavy.
        Works on side effect on nx.Digraph.
        """
        start_node = [node for node in graph.nodes if graph.in_degree(node) == 0][0]
        dfs_edges = list(nx.dfs_edges(graph, source=start_node))
        
        linear_chain = []
        set_of_linear_chains = set()
        for u, v in dfs_edges:
            if graph.out_degree(u) == 1 and graph.in_degree(v) == 1:
                if u not in linear_chain:
                    linear_chain.append(u)

                if v not in linear_chain:
                    linear_chain.append(v)
            else:
                if linear_chain:
                    set_of_linear_chains.add(tuple(linear_chain))
                
                linear_chain = []

        if linear_chain != []:
            set_of_linear_chains.add(tuple(linear_chain))
            linear_chain = []
        
        for chain in set_of_linear_chains:
            node_list = list(chain)
            self._merge_linear_nodes(graph, node_list)

        return
    
    def _merge_parallel_nodes(self, graph: nx.DiGraph, node_list):
        """
        Doesn't return anything since operating on deep copy of graphs might get heavy.
        Works on side effect on nx.Digraph.
        """
        if len(node_list) == 0:
            return
        
        new_node_machine_list = []
        for node in node_list:
            new_node_machine_list.append(graph.nodes[node]["machine_list"])

        # since we are only merging diamonds (same predecessor, same successor) 
        predecessor = list(graph.predecessors(node_list[0]))[0]
        successor = list(graph.successors(node_list[0]))[0]

        new_node_id = "n" + str(new_node_machine_list)
        pre, ret, var = self._get_orchestrator_code_parallel_merge(graph, node_list)
        graph.add_node(new_node_id, pre=pre, ret=ret, var=var, machine_list=[new_node_machine_list])

        for node in node_list:
            graph.remove_node(node)

        graph.add_edge(predecessor, new_node_id)
        graph.add_edge(new_node_id, successor)

        return

    def _collapse_parallel_chains(self, graph: nx.DiGraph):
        """
        Doesn't return anything since operating on deep copy of graphs might get heavy.
        Works on side effect on nx.Digraph.
        """
        start_node = [node for node in graph.nodes if graph.in_degree(node) == 0][0]
        dfs_nodes = list(nx.dfs_preorder_nodes(graph, source=start_node))

        set_of_parallel_chains = set()
        for curr_node in dfs_nodes:
            curr_node_succ = list(graph.successors(curr_node))
            diamond_forming_node = []

            for succ in curr_node_succ:
                if graph.out_degree(succ) == 1:
                    diamond_forming_node.append(succ)

            group_by_succ_dict = defaultdict(list)
            for node in diamond_forming_node:
                succ = list(graph.successors(node))[0]
                group_by_succ_dict[succ].append(node)

            for val in group_by_succ_dict.values():
                if len(val) > 1:
                    set_of_parallel_chains.add(tuple(val))

        for chain in set_of_parallel_chains:
            chain_list = list(chain)
            self._merge_parallel_nodes(graph, chain_list)
        
        return

    def get_orchestrator_code(self):
        """
        Breaker of linear and parallel chains.
        TODO: Actually generate the generated_code from the updated graph
        """
        output_dag = copy.deepcopy(self.__dag)
        while len(output_dag.nodes()) != 1:
            self._collapse_linear_chains(output_dag)
            self._collapse_parallel_chains(output_dag)

        generated_code = """
const composer = require("openwhisk-composer");

module.exports = composer.sequence("/guest/graphs/graph_gen", composer.parallel("/guest/graphs/graph_bft", "/guest/graphs/pagerank", "/guest/graphs/graph_mst"), "/guest/graphs/aggregate");
"""
        return generated_code