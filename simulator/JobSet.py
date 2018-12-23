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
        jobsList = []