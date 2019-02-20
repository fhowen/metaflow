import random
import pandas as pd
import matplotlib.pyplot as plt
def job_fintime(file_1,file_2):
  data_1 = pd.read_csv(file_1)
  data_2 = pd.read_csv(file_2)
  jobfin = [[],[]]
  for i in range(0, len(data_1)):
    row_1 = data_1.loc[i]
    row_2 = data_2.loc[i]
    a=random.random()+1
    jobfin[0].append(row_1[4] - row_1[1])
    jobfin[1].append(row_2[4] - row_2[1])
  plt.grid(ls='--')
  plt.plot(range(len(jobfin[1])),jobfin[1],label='JCT of Varys')
  plt.plot(range(len(jobfin[0])),jobfin[0],label='JCT of MDAG')
  plt.xlabel('Job ID')
  plt.ylabel('Time(ms)')
  plt.legend()
  plt.show()
 
def mach_rate(file_1,file_2):
  interval = 1
  with open(file_1, 'r') as file1_to_read:
    line = file1_to_read.readline()[:-1]
    record1 = line.split(',')
    record1 = [int(i) for i in record1]
  imgrd1 = []
  tempsum = 0
  for i in range(len(record1)):
    tempsum += record1[i]
    if (i+1)%interval == 0:
      imgrd1.append(tempsum/interval)
      tempsum = 0
  with open(file_2, 'r') as file2_to_read:
    line = file2_to_read.readline()[:-1]
    record2 = line.split(',')
    record2 = [int(i) for i in record2]
  imgrd2 = []
  tempsum = 0
  for i in range(len(record2)):
    tempsum += record2[i]
    if (i+1)%interval == 0:
      imgrd2.append(tempsum/interval)
      tempsum = 0  
  
  plt.grid(ls='--')
  plt.plot(range(len(imgrd1)),imgrd1, label='MDAG')
  plt.plot(range(len(imgrd2)),imgrd2, label='VARYS')
  plt.xlabel('Time(ms)')
  plt.ylabel('Working Machine Number')
  plt.legend()
  plt.show()
  


#job_fintime('logjobtime-MDAG.csv','logjobtime-VARYS.csv')
mach_rate('logmachrate-MDAG.csv','logmachrate-VARYS.csv')