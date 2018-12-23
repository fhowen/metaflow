import Constants
from Flow import Flow
from Compu import Compu
import networkx as nx
import random
import matplotlib.pyplot as plt

class Reducer:
    TotalReducerNum = 0
    def __init__(self, reducer_name, total_bytes):
        self.reducerName = reducer_name
        self.reducerID = Reducer.TotalReducerNum
        self.reducerActive = Constants.UNSUBMITTED
        self.finishTime = Constants.MAXTIME
        self.dag = nx.DiGraph()
        self.flowList = []
        self.compuList = []
        self.totalBytes = total_bytes
        Reducer.TotalReducerNum += 1


    def set_attributes(self, location_id, start_time, mapper_list):
        self.locationID = location_id
        self.startTime = start_time
        self.mapperList = mapper_list
        self.mapperNum = len(self.mapperList)


    def addTasks(self, compu_num):
        self.__addFlows()
        self.__addCompus(compu_num)
        
    def __addFlows(self):
        for i in range(0, self.mapperNum):
            f = Flow("F" + self.reducerName[1:] + "-" + str(i), self)
            f.set_attributes(self.mapperList[i], self.locationID, self.totalBytes/self.mapperNum, self.startTime)
            self.flowList.append(f)
            self.dag.add_node(f, mark=f.flowName)
        #self.dag.add_nodes_from(self.flowList)
    
    def __addCompus(self, compu_num):
        for i in range(0, compu_num):
            c = Compu("C" + self.reducerName[1:] + "-" + str(i), self)
            c.set_attributes(self.locationID, random.randint(10, 100))
            self.compuList.append(c)
            self.dag.add_node(c, mark=c.compuName)
        #self.dag.add_nodes_from(self.compuList)

    def bindDag(self, dag_type):
        if dag_type == "DNN":
            assert len(self.compuList) == 2*len(self.flowList), "For DNN reducer, a wrong number of compu tasks"
            self.__bindDnnDag()
        else:
            self.__bindDnnDag()

    def __bindDnnDag(self):
        for i in range(0, len(self.compuList)-1):
            self.dag.add_edge(self.compuList[i],self.compuList[i+1])
        for i in range(0, len(self.flowList)):
            self.dag.add_edge(self.flowList[i], self.compuList[i])


    def plotDag(self):
        labels = nx.get_node_attributes(self.dag,'mark')
        nx.draw(self.dag, labels=labels, with_labels=True)
        plt.show()


r = Reducer("R-0-0", 100)
r.set_attributes(5, 100, [1,2,3])
r.addTasks(6)
print(r.flowList)
print(r.flowList[1].flowID)
print(list(r.dag.nodes(data=True)))
for node in list(r.dag.nodes):
    if isinstance(node, Flow):
        print(node)
r.bindDag(Constants.DNNDAG)

r.plotDag()
