import pandas as pd
import matplotlib.pyplot as plt

# cal for overlap ratio
def ana_1(file_name):
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
    plt.show()
    

            





if __name__ == "__main__":
    log_name = "logfile-10.csv"
    '''
    data = pd.read_csv("logfile2018-12-30-18-51-30.csv")    
    print("Total rows: {0}".format(len(data)))
    print(data.loc[0])
    for i in range(0,3):
        print(i)
    '''
    ana_1(log_name)




