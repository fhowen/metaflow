import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
import networkx as nx
import random
import os

class JobSet:
    def __init__(self):
        self.jobsList = []
    
    def addJob(self, submit_time, mapper_list, reducer_list, data_size_list):
        j = Job()
        j.set_attributes(submit_time, mapper_list, reducer_list, data_size_list)
        self.jobsList.append(j)

    # read coflow trace and add jobs
    def readTrace(self):
        base_dir = os.getcwd()
        f_name = os.path.join(base_dir, '', "coflow_trace_test.txt")
        f = open(f_name, 'r')
        print("Begin to read coflow_trace...")
        for line in f:
            line = line.strip()
            sp_line = line.split(' ')
            submit_time = int(sp_line[1])
            mapper_num = int(sp_line[2])
            cursor = 3 
            mapper_list = []
            for j in range(0, mapper_num):
                mapper_list.append(int(sp_line[cursor]))
                cursor += 1
            reducer_num = int(sp_line[cursor])
            cursor += 1
            reducer_list = []
            data_size_list = []
            #dealing each reducer
            for j in range(0, reducer_num):
                ssp_line = sp_line[cursor].split(':')
                reducer_id = int(ssp_line[0])
                data_size = float(ssp_line[1])*8*1024*1024
                reducer_list.append(reducer_id)
                data_size_list.append(data_size)
                cursor += 1
            self.addJob(submit_time, mapper_list, reducer_list, data_size_list)
        f.close()
    
    #create the uniform dag for one job
    def createOneDag(self, dag_type, mapper_num):
        dag = nx.DiGraph()
        if dag_type == Constants.DNNDAG:
            # create nodes
            for i in range(0, mapper_num):
                dag.add_node(str(i))
            for i in range(mapper_num, 2*mapper_num):
                dag.add_node(str(i))
            # add edges
            # 1. edges from flow to compu
            for i in range(0, mapper_num):
                dag.add_edge(str(i), str(i+mapper_num))
            # 2. edges from compu to compu
            for i in range(mapper_num, 2*mapper_num - 1):
                dag.add_edge(str(i), str(i+1))
            print(dag.edges())
            

    #copy the relationship in pure dag to real dag of reducer
    def copyDag(self, pure_dag):
        pass

    # generate dag relationship and task size
    def genDags(self):
        for j in self.jobsList:
            # generate a dag
            self.createOneDag(Constants.DNNDAG, 4)
            for r in j.reducerList:
                # assign the dag to each reducer
                r.genTasks(len(r.mapperList))
                r.bindDag(Constants.DNNDAG)
                r.initAlphaBeta()

    # store dag to .dot and .txt file
    def storeDag(self):
        for j in self.jobsList:
            for r in j.reducerList:
                #r.dag2Dot()
                r.dag2Txt()

    # read dag from txt file
    def readDag(self):
        for j in self.jobsList:
            for r in j.reducerList:
                r.txt2Dag()
                r.initAlphaBeta()


'''
js.addJob(100, [1,2,3],[4,5,6],[100,200,300])
for i in js.jobsList:
    #i.reducerList[0].plotDag()cd ..
    for j in i.reducerList:
        #j.dag2Dot()
        print("===========")
        print(j.flowList[0])
        j.dag2Txt()
        for m in j.dag.neighbors(j.flowList[0]):
            print(m)

print("===========")
'''
'''
node = js.jobsList[0].reducerList[0].getNodeByMark("F-0-0-0")
print(node)
'''
'''
js.jobsList[0].reducerList[0].txt2Dag()
'''