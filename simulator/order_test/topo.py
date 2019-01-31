import itertools
import networkx as nx
# represent 4,5,6,7
node_list = [0,1,2,3]
# represent edges 
'''
direct edges
   0  1  2  3
 ------------
0 |0        
1 |   0    
2 |      0  
3 |         0
in total , 12 variables
'''
edge_matrix = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]
topo_set = []

def generate_topos():
    topo = []
    for i in itertools.permutations('0123',4):
        topo = []
        topoStr = ' '.join(i)
        for j in range(0,4):
            topo.append(int(topoStr.split(' ')[j]))
        #print(topo)
        topo_set.append(topo)
    print(topo_set)
                    

def topo_num():
    topoNum= 0
    # for each topo sort
    for topo in topo_set:
        # for each edge
        flag = True
        for i in range(0,4):
            for j in range(0,4):
                # if edge exists from i -> j, i must exist before j in topo
                if edge_matrix[i][j] == 1:
                    index_i = topo.index(i)
                    index_j = topo.index(j)
                    if index_i > index_j:
                        flag = False
        if flag == True:
            topoNum += 1
    return topoNum

def judge_loop():
    return False

def save_to_file(num):
    print(num)
    print(edge_matrix)
    pass



def generate_dag():
    i = 0
    j = 0
    for a_1 in range(0,2):
        edge_matrix[0][1] = a_1
        for a_2 in range(0,2):
            edge_matrix[0][2] = a_2
            for a_3 in range(0,2):
                edge_matrix[0][3] = a_3
                for a_4 in range(0,2):
                    edge_matrix[1][0] = a_4
                    if edge_matrix[1][0] + edge_matrix[0][1] == 2:
                        break
                    for a_5 in range(0,2):
                        edge_matrix[1][2] = a_5
                        for a_6 in range(0,2):
                            edge_matrix[1][3] = a_6
                            for a_7 in range(0,2):
                                edge_matrix[2][0] = a_7
                                if edge_matrix[2][0] + edge_matrix[0][2] == 2:
                                    break
                                for a_8 in range(0,2):
                                    edge_matrix[2][1] = a_8
                                    if edge_matrix[2][1] + edge_matrix[1][2] == 2:
                                        break
                                    for a_9 in range(0,2):
                                        edge_matrix[2][3] = a_9
                                        for a_10 in range(0,2):
                                            edge_matrix[3][0] = a_10
                                            if edge_matrix[3][0] + edge_matrix[0][3] == 2:
                                                break
                                            for a_11 in range(0,2):
                                                edge_matrix[3][1] = a_11
                                                if edge_matrix[3][1] + edge_matrix[1][3] == 2:
                                                    break
                                                for a_12 in range(0,2):
                                                    edge_matrix[3][2] = a_12
                                                    if edge_matrix[3][2] + edge_matrix[2][3] == 2:
                                                        break
                                                    i += 1
                                                    has_loop = judge_loop()
                                                    if has_loop:
                                                        continue
                                                    else:
                                                        num = topo_num()
                                                        if num != 0:
                                                            j += 1
                                                        if num == 12:
                                                            save_to_file(num)
    #print(i)
    #print(j)


if __name__ == '__main__':
    generate_topos()
    generate_dag()


