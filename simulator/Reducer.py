import Constants
from Flow import Flow
from Compu import Compu
import networkx as nx
import random
import matplotlib.pyplot as plt
import os

class Reducer:
    TotalReducerNum = 0
    def __init__(self, reducer_name, total_bytes, parent_job):
        self.reducerName = reducer_name
        self.reducerID = Reducer.TotalReducerNum
        self.reducerActive = Constants.UNSUBMITTED
        self.finishTime = Constants.MAXTIME
        self.dag = nx.DiGraph()
        self.flowList = []
        self.compuList = []
        self.totalBytes = total_bytes
        self.parentJob = parent_job
        Reducer.TotalReducerNum += 1


    def set_attributes(self, location_id, start_time, mapper_list):
        self.locationID = location_id
        self.startTime = start_time
        self.mapperList = mapper_list
        self.mapperNum = len(self.mapperList)
        # compu tasks number  
        #self.addTasks(2*len(mapper_list))
        #self.bindDag(Constants.DNNDAG)


    def genTasks(self, compu_num):
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
    
    def dag2Dot(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.reducerName + ".dot")
        f_open = open(file_name, 'w')
        f_open.write('digraph g {\n')
        marks = nx.get_node_attributes(self.dag,'mark')
        for i,j in self.dag.edges():
            f_open.write('\t'+"\"" +marks[i] +"\""+ '->' +"\""+marks[j] +"\""+ '\n')
        f_open.write('}\n')
        f_open.close()
    
    def dag2Txt(self):
        marks = nx.get_node_attributes(self.dag,'mark')
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.reducerName + ".txt")
        f_open = open(file_name, 'w')
        f_open.write(str(len(self.compuList)) + '\n')
        for i in self.flowList:
            temp_str = i.flowName
            temp_str += " "
            temp_str += str(i.flowSize)
            for j in self.compuList:
                if self.dag.has_edge(i,j):
                    temp_str += " "
                    temp_str += j.compuName
            temp_str += "\n"
            f_open.write(temp_str)
        for i in self.compuList:
            temp_str = i.compuName
            temp_str += " "
            temp_str += str(i.compuSize)
            for j in self.compuList:
                if self.dag.has_edge(i,j):
                    temp_str += " "
                    temp_str += j.compuName
            temp_str += "\n"
            f_open.write(temp_str)

        f_open.close()
        
    def getNodeByMark(self, mark_str):
        marks = nx.get_node_attributes(self.dag,'mark')
        for i in self.dag.nodes():
            if marks[i] == mark_str:
                return i
        return NULL
        
    
    def txt2Dag(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.reducerName + ".txt")
        print("Read file %s"%(file_name))
        f_open = open(file_name, 'r')
        #first line, number of compu tasks
        self.genTasks(int(f_open.readline().strip()))
        # each line for each flow node in dag, assign size and relationships
        for i in range(0, len(self.flowList)):
            line = f_open.readline().strip()
            sp_line = line.split(' ')
            node = self.getNodeByMark(sp_line[0])
            node.flowSize = float(sp_line[1])
            if len(sp_line) > 2:
                for child in sp_line[2:]:
                    child_node = self.getNodeByMark(child)
                    self.dag.add_edge(node, child_node)
        # each line for each compu node in dag, assign size and relationships
        for i in range(0, len(self.compuList)):
            line = f_open.readline().strip()
            sp_line = line.split(' ')
            node_size = sp_line[1]
            node = self.getNodeByMark(sp_line[0])
            node.compuSize = float(sp_line[1])
            if len(sp_line) > 2:
                for child in sp_line[2:]:
                    child_node = self.getNodeByMark(child)
                    self.dag.add_edge(node, child_node)
        f_open.close()
    

'''
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
'''