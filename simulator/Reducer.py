import Constants
from Flow import Flow
from Compu import Compu
import networkx as nx

class Reducer:
    def __init__(self, reducer_name, reducer_id, total_bytes):
        self.reducerName = reducer_name
        self.reducerID = reducer_id
        self.reducerActive = 0
        self.finishTime = Constants.MAXTIME
        self.dag = nx.Graph()
        self.flowList = []
        self.compuList = []
        self.totalBytes = total_bytes


    def set_attributes(self, location_id, start_time, mapper_list):
        self.locationID = location_id
        self.startTime = start_time
        self.mapperList = mapper_list
        for i in range(0, len(mapper_list)):
            self.__addFlow(i, self.mapperList[i], self.totalBytes/len(self.mapperList))
        
        
    def __addFlow(self, flow_id, src_id, flow_size):
        f = Flow("tt", flow_id)
        self.flowList.append(f)


r = Reducer("11", 5, 100)
r.set_attributes(5, 100, [1,2])
print(r.flowList)
        