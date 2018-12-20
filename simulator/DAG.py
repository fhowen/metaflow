from igraph import *

class Cdag:
    totalCdagNum = 0

    def __init__(self):
        self.dag = Graph(directed=True)
        Cdag.totalCdagNum += 1

    def set_dag(self, input_dag):
        self.dag = input_dag

    def generate_dag(self, type, mapper_num):
        # DNN training 
        if type == 1:
            generate_dnn_dag(self, mapper_num)
            
    def generate_dnn_dag(self, layer_num):
        self.dag.add_vertices(2*layer_num)
        for i in range(0, 2*layer_num-1):
            self.dag.add_edge(i, i+1)
        for i in range(0, layer_num-1):
            self.dag.add_edge(i, 2*layer_num-i-1)

    #def read_from_file(self):    


test1 = Cdag()
test1.generate_dnn_dag(6)
layout = test1.dag.layout(circle)
print(test1.dag, )
#plot(test1.dag)
test2 = Cdag()
#test2.setdag(test1.dag)
#plot(test2.dag)
print("Total Cdag number is %d" % Cdag.totalCdagNum)
