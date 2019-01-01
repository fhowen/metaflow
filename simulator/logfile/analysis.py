import pandas as pd
import matplotlib.pyplot as plt

# cal for overlap ratio
def rack_overlap(file_name):
    IO = file_name
    data = pd.read_csv(IO)    
    # 450, 150
    print("Total rows: {0}".format(len(data)))
    overLapList = []
    # cal for each rack
    for i in range(0, len(data), 3):
        #print(i)
        overlap = 1
        send = data.loc[i]
        recv = data.loc[i + 1]
        comp = data.loc[i + 2]
        #print(send[0])
        # cal for each time slot
        count_r = 0
        count_c = 0
        count_o = 0
        for j in range(1, data.shape[1] - 1):
            if recv[j] > 0:
                count_r += 1
            if comp[j] > 0:
                count_c += 1
            if recv[j] * comp[j] > 0:
                count_o += 1
        # cal overlap, best 0.5, worst 1
        if count_r + count_c != 0:
            overlap = (count_r + count_c - count_o)/(count_r + count_c)
        overLapList.append(overlap)
    #print(overLapList)
    ave_overlap = sum(overLapList)/len(overLapList)
    # plot
    plt.scatter(range(0, 150), overLapList, s=50, alpha=.5)
    plt.text(0, 1.05, 'Average: ' + str(ave_overlap), fontsize=10)
    plt.savefig('overMDAG.png')
    #plt.show()


def job_fintime(file_1, file_2, file_3):
    data_1 = pd.read_csv(file_1)
    data_2 = pd.read_csv(file_2)
    data_3 = pd.read_csv(file_3)
    #print("Total rows: {0}".format(len(data)))
    flowfin = [[],[],[]]
    jobfin = [[],[],[]]
    # for each job
    for i in range(0, len(data_1)):
        row_1 = data_1.loc[i]
        row_2 = data_2.loc[i]
        row_3 = data_3.loc[i]
        flowfin[0].append(row_1[3] - row_1[1])
        flowfin[1].append(row_2[3] - row_2[1])
        flowfin[2].append(row_3[3] - row_3[1])
        jobfin[0].append(row_1[4] - row_1[1])
        jobfin[1].append(row_2[4] - row_2[1])
        jobfin[2].append(row_3[4] - row_3[1])
    # sum time of flow and job finish
    flowsum = []
    jobsum = []
    for i in range(0,3):
        flowsum.append(sum(flowfin[i]))
        jobsum.append(sum(jobfin[i]))
    for i in range(0,3):
        flowsum[i] = flowsum[i]/len(flowfin[0])
        jobsum[i] = jobsum[i]/len(flowfin[0])

    plt.yscale('log')
    x =list(range(1,len(flowfin[0]) + 1))
    total_width, n = 0.8, 3
    width = total_width / n
    plt.bar(x,flowfin[0],label='MDAG',width=width,fc='g')
    for a,b in zip(x,flowfin[0]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11) 
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x,flowfin[1],label='SEBF',width=width,fc='r')
    for a,b in zip(x,flowfin[1]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11)
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x,flowfin[2],label='FIFO',width=width,fc='b')
    for a,b in zip(x,flowfin[2]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11)
    plt.legend()
    plt.text(1, 100000, 'flowfin: ' + str(flowsum[0]) + " " + str(flowsum[1]) + " " + str(flowsum[2]), fontsize=10)
    #plt.show()
    plt.savefig('flowfin.png')
    
    plt.cla()

    plt.yscale('log')
    x =list(range(1,len(flowfin[0]) + 1))
    total_width, n = 0.8, 3
    width = total_width / n
    plt.bar(x,jobfin[0],label='MDAG',width=width,fc='g')
    for a,b in zip(x,jobfin[0]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11)
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x,jobfin[1],label='SEBF',width=width,fc='r')
    for a,b in zip(x,jobfin[1]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11)
    for i in range(len(x)):
        x[i] = x[i] + width
    plt.bar(x,jobfin[2],label='FIFO',width=width,fc='b')
    for a,b in zip(x,jobfin[2]):  
        plt.text(a, b+0.05, '%.0f' % b, ha='center', va= 'bottom',fontsize=11)
    plt.legend()
    plt.text(1, 100000, 'jobfin: ' + str(jobsum[0]) + " " + str(jobsum[1]) + " " + str(jobsum[2]), fontsize=10)
    #plt.show()
    plt.savefig('jobfin.png')
    plt.cla()


if __name__ == "__main__":
    
    #overlap computation
    '''
    log_name = "logfile2018-12-31-12-23-45.csv"
    rack_overlap(log_name)
    '''

    # job finish time comparision
    file_1 = "logjobtime-MDAG2018-12-31-13-03-45.csv"
    file_2 = "logjobtime-SEBF2018-12-31-13-04-01.csv"
    file_3 = "logjobtime-FIFO2018-12-31-12-12-50.csv"
    job_fintime(file_1, file_2, file_3)
    




