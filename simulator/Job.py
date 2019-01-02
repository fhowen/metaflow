import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
import networkx as nx
import random
import os

class Job:
    __slots__ = ['jobName', 'jobID', 'jobActive', 'submitTime', \
                 'startTime', 'finishTime', 'flowFinishTime', \
                 'finReducerNum', 'reducerList', 'mapperList', 'dag']
    #job index from 1
    TotalJobNum = 1
    def __init__(self):
        self.jobName = "J-" + str(Job.TotalJobNum)
        self.jobID = Job.TotalJobNum
        self.jobActive = Constants.UNSUBMITTED
        self.submitTime = Constants.MAXTIME
        self.startTime = Constants.MAXTIME
        self.finishTime = Constants.MAXTIME
        self.flowFinishTime = Constants.MAXTIME
        self.finReducerNum = 0
        self.reducerList = []
        self.dag = nx.DiGraph()
        Job.TotalJobNum += 1
    
    def set_attributes(self, submit_time, mapper_list, reducer_list, data_size_list):
        self.submitTime = submit_time
        self.mapperList = mapper_list
        for i in range(0, len(reducer_list)):
            r = Reducer("R"+self.jobName[1:] +"-"+str(i), data_size_list[i], self)
            r.set_attributes(reducer_list[i], self.submitTime, mapper_list)
            self.reducerList.append(r)


    def dag2Dot(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.jobName + ".dot")
        f_open = open(file_name, 'w')
        f_open.write('digraph g {\n')
        for i,j in self.dag.edges():
            f_open.write('\t'+"\"" + str(i) +"\""+ '->' +"\""+ str(j) +"\""+ '\n')
        f_open.write('}\n')
        f_open.close()
    
    def dag2Txt(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.jobName + ".txt")
        f_open = open(file_name, 'w')
        f_open.write(str(self.dag.number_of_nodes() - len(self.mapperList)) + '\n')
        for i in range(0, len(self.mapperList)):
            temp_str = str(i)
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
'''
j = Job()
j.set_attributes(100, [1,2],[3,4],[100,300])
for i in j.reducerList:
    print(i.reducerName)
'''