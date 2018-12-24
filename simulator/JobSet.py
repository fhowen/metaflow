import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
#import networkx as nx
import random

class JobSet:
    TotalJobSetNum = 0
    def __init__(self):
        self.jobsList = []
    
    def addJob(self, submit_time, mapper_list, reducer_list, data_size_list):
        j = Job()
        j.set_attributes(submit_time, mapper_list, reducer_list, data_size_list)
        self.jobsList.append(j)

    def readTrace(file_name):
        pass

js = JobSet()
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

node = js.jobsList[0].reducerList[0].getNodeByMark("F-0-0-0")
print(node)