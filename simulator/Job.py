import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
#import networkx as nx
import random

class Job:
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
        Job.TotalJobNum += 1
    
    def set_attributes(self, submit_time, mapper_list, reducer_list, data_size_list):
        self.submitTime = submit_time
        self.mapperList = mapper_list
        for i in range(0, len(reducer_list)):
            r = Reducer("R"+self.jobName[1:] +"-"+str(i), data_size_list[i], self)
            r.set_attributes(reducer_list[i], self.submitTime, mapper_list)
            self.reducerList.append(r)

'''
j = Job()
j.set_attributes(100, [1,2],[3,4],[100,300])
for i in j.reducerList:
    print(i.reducerName)
'''