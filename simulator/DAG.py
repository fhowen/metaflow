from igraph import *
from Flow import Flow
class Cdag:
    totalCdagNum = 0
    __color_dict = {"c": "grey", "f": "pink"}

    def __init__(self, mapper_list, flow_size_list, machine_id, start_time):
        self.machine_id = machine_id
        self.start_time = start_time

        self.dag = Graph(directed=True)
        Cdag.totalCdagNum += 1



    def set_dag(self, input_dag):
        self.dag = input_dag

    def generate_dag(self, type, mapper_num):
        # DNN training 
        if type == 1:
            generate_dnn_dag(self, mapper_num)
        else:
            generate_dnn_dag(self, mapper_num)
            
    def generate_dnn_dag(self, layer_num):
        self.dag.add_vertices(3*layer_num)
        for i in range(0, layer_num):
            self.dag.vs[i]["tag"] = "f_"+str(i)
            self.dag.vs[i]["color"] = Cdag.__color_dict["f"]
            self.dag.add_edge(i, i+layer_num)
        for i in range(layer_num, 3*layer_num):
            self.dag.vs[i]["tag"] = "c_"+str(i-layer_num)
            self.dag.vs[i]["color"] = Cdag.__color_dict["c"]
        for i in range(layer_num, 3*layer_num-1):
            self.dag.add_edge(i, i+1)
        for i in range(layer_num, 2*layer_num-1):
            self.dag.add_edge(i,4*layer_num-i-1)

    def plot_dag(self):
        layout = self.dag.layout("kk")
        self.dag.vs["label"] = self.dag.vs["tag"]
        plot(self.dag, "dag.png", layout = layout)
        

    #def read_from_file(self):    


test1 = Cdag()
test1.generate_dnn_dag(6)
test1.plot_dag()

