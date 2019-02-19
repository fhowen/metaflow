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
                 'finReducerNum', 'reducerList', 'mapperList', \
                 'dag', 'dagType', 'expectedTime', 'cfExpectedtime']
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
        self.dagType = ''
        self.expectedTime = 0.0
        self.cfExpectedtime = 0.0
        Job.TotalJobNum += 1
    
    def set_attributes(self, submit_time, mapper_list, reducer_list, data_size_list):
        self.submitTime = submit_time
        self.mapperList = mapper_list
        flow_sum = 0
        for i in range(0, len(reducer_list)):
            r = Reducer("R"+self.jobName[1:] +"-"+str(i), data_size_list[i], self)
            flow_sum += data_size_list[i]
            r.set_attributes(reducer_list[i], self.submitTime, mapper_list)
            self.reducerList.append(r)
        c2fRatio = random.choice(Constants.C2F_LIST)
        compu_sum = (Constants.RACK_COMP_PER_SEC * flow_sum / Constants.RACK_BITS_PER_SEC )* c2fRatio
        for r in self.reducerList:
            r.totalFlops = compu_sum/len(self.reducerList)

    def updateExpectedTime(self):
        maxflow = 0.0
        maxcomp = 0.0
        ranksend = {}
        rankrecv = {}
        for reducer in self.reducerList:
            flowlist = reducer.flowList
            for flow in flowlist:
                srack = flow.srcID
                rrack = flow.dstID
                if srack in ranksend.keys():
                    ranksend[srack] += flow.remainSize
                else:
                    ranksend[srack] = flow.remainSize
                if ranksend[srack] > maxflow:
                    maxflow = ranksend[srack]
                if rrack in rankrecv.keys():
                    rankrecv[rrack] += flow.remainSize
                else:
                    rankrecv[rrack] = flow.remainSize
                if rankrecv[rrack] > maxflow:
                    maxflow = rankrecv[rrack]
            complist = reducer.compuList
            compsize = 0.0
            for comp in complist:
                compsize = compsize + comp.remainSize
            if compsize > maxcomp:
                maxcomp = compsize
        #Note: exceptedTime = flowtime+comptime
        #self.expectedTime = maxflow/Constants.RACK_BITS_PER_SEC \
        #                    + maxcomp/Constants.RACK_COMP_PER_SEC
        #Note: exceptedTime = max(flowtime, comptime)
        self.expectedTime = max(maxflow/Constants.RACK_BITS_PER_SEC, \
                            maxcomp/Constants.RACK_COMP_PER_SEC)
        self.cfExpectedtime = maxflow/Constants.RACK_BITS_PER_SEC
    # update alpha beta with each flow    
    def updateAlphaBeta(self):
        for reducer in self.reducerList:
            if reducer.reducerActive == Constants.SUBMITTED or \
                reducer.reducerActive == Constants.STARTED:
                reducer.updateAlphaBeta()

    # TODO update alpha beta with metaflow
    def updateMFAlphaBeta(self):
        metaflowList = []
        # 1 extract one metaflow
        #for i in range(0, len(self.reducerList[0].compuList)):

        # 2 calculate the alpha beta for metaflow
        pass

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
        mapper_num = len(self.mapperList)
        compu_num = self.dag.number_of_nodes() - mapper_num
        file_name = os.path.join(base_dir, 'dags', self.jobName + ".txt")
        f_open = open(file_name, 'w')
        f_open.write(str(self.dagType) + " " + str(mapper_num) + ' ' + str(compu_num) + '\n')
        for i in range(0, mapper_num):
            temp_str = str(i)
            #temp_str += " "
            #temp_str += str(self.dag.node[i]['size'])
            for j in range(0, compu_num):
                if self.dag.has_edge(i,j+mapper_num):
                    temp_str += " "
                    temp_str += str(j+mapper_num)
            temp_str += "\n"
            f_open.write(temp_str)
        for i in range(0, compu_num):
            temp_str = str(i+mapper_num)
            #temp_str += " "
            #temp_str += str(self.dag.node[i+mapper_num]['size'])
            for j in range(i, compu_num):
                if self.dag.has_edge(i+mapper_num,j+mapper_num):
                    temp_str += " "
                    temp_str += str(j+mapper_num)
            temp_str += "\n"
            f_open.write(temp_str)
        f_open.close()

    def txt2Dag(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.jobName + ".txt")
        print("Read file %s"%(file_name))
        f_open = open(file_name, 'r')
        #first line: type mapper_num compu_num
        line = f_open.readline().strip()
        sp_line = line.split(' ')
        #print(sp_line)
        self.dagType = sp_line[0]
        mapper_num = int(sp_line[1])
        compu_num = int(sp_line[2])
        # add all nodes
        for i in range(0, mapper_num + compu_num):
            self.dag.add_node(i)
        # each line for each flow in dag, assign size and relationships
        for i in range(0, mapper_num + compu_num):
            line = f_open.readline().strip()
            sp_line = line.split(' ')
            # this node has children
            if len(sp_line) > 1:
                for j in range(1, len(sp_line)):
                    self.dag.add_edge(i, int(sp_line[j])) 
        f_open.close()
        return self.dagType, compu_num

    def storeSize(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.jobName + "_SIZE.txt")
        f_open = open(file_name, 'w')
        for r in self.reducerList:
            for i in r.flowList:
                f_open.write(str(i.flowSize)+' ')
            for j in r.compuList:
                f_open.write(str(j.compuSize)+' ')
            f_open.write("\n")
        f_open.close()
    
    def readSize(self):
        base_dir = os.getcwd()
        file_name = os.path.join(base_dir, 'dags', self.jobName + "_SIZE.txt")
        print("Read SIZE file %s"%(file_name))
        f_open = open(file_name, 'r')
        #each line for one reducer
        for i in range(0, len(self.reducerList)):
            line = f_open.readline().strip()
            sp_line = line.split(' ')
            cursor = 0
            for f in self.reducerList[i].flowList:
                f.flowSize = float(sp_line[cursor])
                f.remainSize = f.flowSize
                cursor += 1
            for c in self.reducerList[i].compuList:
                c.compuSize = float(sp_line[cursor])
                c.remainSize = c.compuSize
                cursor += 1
        f_open.close()
'''
j = Job()
j.set_attributes(100, [1,2],[3,4],[100,300])
for i in j.reducerList:
    print(i.reducerName)
'''