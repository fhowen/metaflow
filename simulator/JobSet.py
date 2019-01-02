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
    
    #set size, alpha, bebta
    def dagAttrs(self, job, dag_type):
        mapper_num = len(job.mapperList)
        #1 set alpha and beta
        if dag_type == Constants.DNNDAG:
            for i in range(0, mapper_num):
                # set alpha
                job.dag.node[i]['alpha'] = mapper_num - i
                # set beta
                job.dag.node[i]['beta'] = mapper_num - i
        elif dag_type == Constants.WEBDAG:
            pass
        else:
            pass


    #create the uniform dag for one job
    def createOneDag(self, dag_type, mapper_num):
        dag = nx.DiGraph()
        compu_num = 0
        if dag_type == Constants.DNNDAG:
            compu_num = mapper_num
            # create nodes, flow nodes followed by compu nodes
            for i in range(0, mapper_num):
                dag.add_node(i)
            for i in range(mapper_num, 2*mapper_num):
                dag.add_node(i)
            # add edges
            # 1. edges from flow to compu
            for i in range(0, mapper_num):
                dag.add_edge(i, i+mapper_num)
            # 2. edges from compu to compu
            for i in range(mapper_num, 2*mapper_num - 1):
                dag.add_edge(i, i+1)
            #print(dag.edges())
        elif dag_type == Constants.WEBDAG:
            compu_num = 1
            # mapper_num flow and 1 compu
            for i in range(0, mapper_num + 1):
                dag.add_node(i)
            # add edges
            for i in range(0, mapper_num):
                dag.add_edge(i, mapper_num)
        # random dag
        elif dag_type == Constants.RANDOMDAG:
            proba = 0.05
            nodes_matrix = []
            temp = []
            node_rank = 0
            # mapper num flow 
            for i in range(0, mapper_num):
                temp.append(i)
                dag.add_node(i)
            # add to matrix    
            node_rank += mapper_num
            nodes_matrix.append(temp)
            temp = []
            # compu nodes
            layer_num = random.randint(3, 10)
            # for each layer
            for i in range(1, layer_num + 1):
                # root layer
                if i == 1:
                    nodes_num = random.randint(1, mapper_num)
                # not root layer
                else:
                    nodes_num = random.randint(1, mapper_num)
                # add nodes
                compu_num += nodes_num
                for j in range(node_rank, node_rank + nodes_num):
                    temp.append(j)
                    dag.add_node(j)
                # forward and reset
                nodes_matrix.append(temp)
                node_rank += nodes_num
                temp = []
                # add edges
                #1 add edges for root layer
                if i == 1:
                    for j in nodes_matrix[0]:
                        dag.add_edge(j, j%len(nodes_matrix[1]) + mapper_num)
                #2 add edges for each layer, not root layer
                if i > 1:
                    b_cur = mapper_num
                    f_cur = nodes_matrix[i][0] - 1
                    # for each node in this layer, if not root layer
                    for j in nodes_matrix[i]:
                        # for each nodes in previous layer, only root layer can connect with flows
                        for k in range(b_cur, f_cur + 1):
                            if random.uniform(0, 1) < proba:
                                dag.add_edge(k, j)
                        # check in case no edge is added
                        if (len(dag.pred[j]) == 0):
                            # random choose a node as father
                            father_node = random.randint(b_cur, f_cur)
                            dag.add_edge(father_node, j)
            
        else:
            pass
        return dag, compu_num
            

    #copy the relationship in pure dag to real dag of reducer
    def copyDag(self, pure_dag, reducer, dag_type):
        # generate flow tasks and compu tasks
        compu_num = pure_dag.number_of_nodes() - len(reducer.mapperList)
        reducer.dagType = dag_type
        # copy edges 
        for u,v in pure_dag.edges():
            #print(u,v)
            v = v - reducer.mapperNum
            # 1 flow --> compu
            if u < reducer.mapperNum:
                reducer.dag.add_edge(reducer.flowList[u], \
                                     reducer.compuList[v])
            # 2 compu --> compu 
            else:
                u = u - reducer.mapperNum
                reducer.dag.add_edge(reducer.compuList[u], \
                                     reducer.compuList[v])
        # copy attrs

    # === TO DO ====== where to assign the compute size ?
    # generate dag relationship and task size
    def genDags(self):
        for j in self.jobsList:
            # generate a dag
            # dag_type = Constants.DNNDAG
            # dag_type = Constants.WEBDAG
            dag_type = Constants.RANDOMDAG
            j.dag, compu_num = self.createOneDag(dag_type, len(j.mapperList))
            self.dagAttrs(j, dag_type)
            
            for r in j.reducerList:
                # assign the dag to each reducer
                r.genTasks(compu_num)
                self.copyDag(j.dag, r, dag_type)
                #r.bindDag(Constants.DNNDAG)
                #r.initAlphaBeta()
                r.copyDagAttrs(dag_type)

    # store dag to .dot and .txt file
    def storeDag(self):
        for j in self.jobsList:
            for r in j.reducerList:
                r.dag2Dot()
                #r.dag2Txt()

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