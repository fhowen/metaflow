import Constants
import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
#import networkx as nx
import random

class Job:
    TotalJobNum = 0
    def __init__(self, job_name):
        self.jobName = job_name
        self.jobID = Job.TotalJobNum
        self.jobActive = Constants.UNSUBMITTED
        self.submitTime = Constants.MAXTIME
        self.startTime = Constants.MAXTIME
        self.finishTime = Constants.MAXTIME
        self.flowFinishTime = Constants.MAXTIME
        reducerList = []
        Job.TotalJobNum += 1
    
    def set_attributes(self):
        pass