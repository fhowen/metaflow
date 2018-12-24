import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
#import networkx as nx
import random
import os

class JobSet:
    TotalJobSetNum = 0
    def __init__(self):
        self.jobsList = []
    
    def addJob(self, submit_time, mapper_list, reducer_list, data_size_list):
        j = Job()
        j.set_attributes(submit_time, mapper_list, reducer_list, data_size_list)
        self.jobsList.append(j)

    def readTrace(self):
        base_dir = os.getcwd()
        f_name = os.path.join(base_dir, '', "coflow_trace.txt")
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
                data_size = float(ssp_line[1])
                reducer_list.append(reducer_id)
                data_size_list.append(data_size)
            self.addJob(submit_time, mapper_list, reducer_list, data_size_list)
        f.close()
    
    def genDag(self):
        for j in self.jobsList:
            for r in j.reducerList:
                r.genTasks(2*len(r.mapperList))

    def storeDag(self):
        for j in self.jobsList:
            for r in j.reducerList:
                #r.dag2Dot()
                r.dag2Txt()

    def readDag(self):
        pass

js = JobSet()
js.readTrace()
print(Flow.TotalFlowNum)
js.genDag()
print(Flow.TotalFlowNum)
js.storeDag()
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