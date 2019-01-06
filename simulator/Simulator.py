import Constants
import Job
from RackInfo import RackInfo
import time
import os
import random
class Simulator:
    def __init__(self, jobset, algorithm="MDAG"):
        self.algorithm = algorithm
        self.datetime = time.time()
        self.rackstatic_sendbps = []
        self.rackstatic_recvbps = []
        self.rackstatic_cps = []
        self.jobset = jobset
        self.numActiveJobs = 0
        self.active_jobs = []
        self.active_flows = []
        self.active_Compus = []
        self.sendBpsFree = []
        self.recvBpsFree = []
        self.rackCpsFree = []
        self.rackinfos = []
        self.FinishedJobs = []
        self.static_recv = 0
        self.static_comp = 0
        self.static_recvcomp = 0
        self.CURRENT_TIME = 0
        for i in range(Constants.MACHINENUM):
            self.sendBpsFree.append(Constants.RACK_BITS_PER_SEC)
            self.recvBpsFree.append(Constants.RACK_BITS_PER_SEC)
            self.rackCpsFree.append(Constants.RACK_COMP_PER_SEC)
            self.rackinfos.append(RackInfo())
            self.rackstatic_sendbps.append([])
            self.rackstatic_recvbps.append([])
            self.rackstatic_cps.append([])

    def ActiveJobAdd(self, job):
        #我们这里直接按照Job的时间顺序进行插入，后面可能设计到其他的插入排序方法
        self.active_jobs.append(job)
        #对添加的job中的reducer设置为active
        for i in range(len(job.reducerList)):
            job.reducerList[i].reducerActive = Constants.SUBMITTED
    
    def SortActiveFLows(self):
        self.active_flows.sort(key=lambda x:(self.active_jobs.index(x.parentJob),-x.alpha,-x.beta))
    
    def resetBpsCpsFree(self):
        for i in range(Constants.MACHINENUM):
            self.sendBpsFree[i] = Constants.RACK_BITS_PER_SEC
            self.recvBpsFree[i] = Constants.RACK_BITS_PER_SEC
            self.rackCpsFree[i] = Constants.RACK_COMP_PER_SEC
            self.rackinfos[i].resetinfo()

    def debug_info(self, level = 0):
        print("CURRENT:",self.CURRENT_TIME/1000)
        print("Active Jobs:",end=' ')
        for activeJob in self.active_jobs:
            print(activeJob.jobName,end=',')
        print("\nFinished Jobs",end=' ')
        for finJob in self.FinishedJobs:
            print(finJob,end=',')   
        if level >= 1: 
            print("\n######Detail Info######")
        count = 0
        for rackinfo in self.rackinfos:
            if rackinfo.UsedSendBpsPro or rackinfo.UsedRecvBpsPro or rackinfo.UsedCpsPro:
                if level >= 1:
                    print("RACKID:",count)
                    print(rackinfo.UsedSendBpsPro)
                    print(rackinfo.UsedRecvBpsPro)
                    print(rackinfo.UsedCpsPro)
                    print("-------------")
                if rackinfo.UsedRecvBpsPro:
                    self.static_recv += 1
                if rackinfo.UsedCpsPro:
                    self.static_comp += 1
                if rackinfo.UsedCpsPro and rackinfo.UsedRecvBpsPro:
                    self.static_recvcomp += 1
            count += 1    
        print("\n#########################")
    def logjobtime(self):
        logfile = "logjobtime-" + self.algorithm + ".csv"
        logpath = os.path.join(Constants.LOGDIR,logfile)
        f = open(logpath, 'w')
        f.write("Job-ID,submitTime,startTime,flowFinTime,FinTime,\n")
        for job in self.jobset.jobsList:
            f.write(str(job.jobID) + "," + \
                    str(job.submitTime) +"," + \
                    str(job.startTime) +"," + \
                    str(job.flowFinishTime) +"," + \
                    str(job.finishTime) + ",\n")
        f.close()

    def savelog(self, wlength):
        logfile = "logfile"+time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(self.datetime))+".csv"
        logpath = os.path.join(Constants.LOGDIR,logfile)
        wid = int(self.CURRENT_TIME/wlength)
        for index in range(len(self.rackinfos)):
            if self.CURRENT_TIME%wlength == 0:
                self.rackstatic_sendbps[index].append(0)
                self.rackstatic_recvbps[index].append(0)
                self.rackstatic_cps[index].append(0)
            if self.rackinfos[index].UsedSendBpsPro:
                self.rackstatic_sendbps[index][wid] += 1
            if self.rackinfos[index].UsedRecvBpsPro:
                self.rackstatic_recvbps[index][wid] += 1
            if self.rackinfos[index].UsedCpsPro:
                self.rackstatic_cps[index][wid] += 1
        if self.CURRENT_TIME%wlength == 0:
            f = open(logpath, 'w')
            f.write("rackid,")
            for i in range(wid):
                f.write(str(i)+",")
            f.write("\n")
            for i in range(Constants.MACHINENUM):
                f.write("rack-"+str(i)+"-send,")
                for j in range(wid):
                    f.write(str(self.rackstatic_sendbps[i][j])+",")
                f.write("\n")
                f.write("rack-"+str(i)+"-recv,")
                for j in range(wid):
                    f.write(str(self.rackstatic_recvbps[i][j])+",")
                f.write("\n")
                f.write("rack-"+str(i)+"-comp,")
                for j in range(wid):
                    f.write(str(self.rackstatic_cps[i][j])+",")
                f.write("\n")
            f.close()

    def SEBF_Distribution(self, EPOCH_IN_MILLIS):
        CurJid = -1
        s = 0
        maxalpha = -1
        maxflow = -1
        for count in range(len(self.active_flows)+1):
            if len(self.active_flows) == 0:
                break
            if count==len(self.active_flows):
                i = len(self.active_flows)-1
            else:
                i = count
            self.active_flows[i].currentBps = 0.0
            SendRack = self.active_flows[i].srcID
            RecvRack = self.active_flows[i].dstID
            supportBps = min(self.sendBpsFree[SendRack], self.recvBpsFree[RecvRack])
            if supportBps > Constants.ZERO:
                self.active_flows[i].alpha = (self.active_flows[i].remainSize)/supportBps
            else:
                self.active_flows[i].alpha = -1
            if CurJid!=self.active_flows[i].parentJob.jobID or count == len(self.active_flows):
                if maxalpha!=-1:
                    srack = self.active_flows[maxflow].srcID
                    rrack = self.active_flows[maxflow].dstID
                    remainBps = min(self.sendBpsFree[srack], self.recvBpsFree[rrack])
                    if remainBps>Constants.ZERO:
                        if self.active_flows[maxflow].parentJob.startTime>=Constants.MAXTIME:
                            self.active_flows[maxflow].parentJob.startTime = self.CURRENT_TIME
                        if self.active_flows[maxflow].parentReducer.startTime>=Constants.MAXTIME:
                            self.active_flows[maxflow].parentReducer.startTime = self.CURRENT_TIME
                        if self.active_flows[maxflow].startTime>=Constants.MAXTIME:
                            self.active_flows[maxflow].startTime = self.CURRENT_TIME
                        self.active_flows[maxflow].currentBps = \
                            min(remainBps,(self.active_flows[maxflow].remainSize)/(EPOCH_IN_MILLIS/1000))
                        self.sendBpsFree[srack] -= self.active_flows[maxflow].currentBps
                        self.recvBpsFree[rrack] -= self.active_flows[maxflow].currentBps
                        self.rackinfos[srack].UsedSendBpsPro[self.active_flows[maxflow].flowName] = \
                                    self.active_flows[maxflow].currentBps/Constants.RACK_BITS_PER_SEC
                        self.rackinfos[rrack].UsedRecvBpsPro[self.active_flows[maxflow].flowName] = \
                                    self.active_flows[maxflow].currentBps/Constants.RACK_BITS_PER_SEC
                while s<count:
                    if s!=maxflow and maxalpha!=-1:
                        srack = self.active_flows[s].srcID
                        rrack = self.active_flows[s].dstID
                        remainBps = min(self.sendBpsFree[srack], self.recvBpsFree[rrack])
                        if remainBps>Constants.ZERO:
                            if self.active_flows[s].parentJob.startTime>=Constants.MAXTIME:
                                self.active_flows[s].parentJob.startTime = self.CURRENT_TIME
                            if self.active_flows[s].parentReducer.startTime>=Constants.MAXTIME:
                                self.active_flows[s].parentReducer.startTime = self.CURRENT_TIME
                            if self.active_flows[s].startTime>=Constants.MAXTIME:
                                self.active_flows[s].startTime = self.CURRENT_TIME
                            idealbps = (self.active_flows[s].remainSize)/(EPOCH_IN_MILLIS/1000)
                            self.active_flows[s].currentBps = min(idealbps, remainBps)
                            self.sendBpsFree[srack] -= self.active_flows[s].currentBps
                            self.recvBpsFree[rrack] -= self.active_flows[s].currentBps
                            self.rackinfos[srack].UsedSendBpsPro[self.active_flows[s].flowName] = \
                                    self.active_flows[s].currentBps/Constants.RACK_BITS_PER_SEC
                            self.rackinfos[rrack].UsedRecvBpsPro[self.active_flows[s].flowName] = \
                                    self.active_flows[s].currentBps/Constants.RACK_BITS_PER_SEC                        
                    s += 1
                maxalpha = self.active_flows[i].alpha
                maxflow = i
                CurJid = self.active_flows[i].parentJob.jobID
            else:
                if self.active_flows[i].alpha > maxalpha:
                    maxalpha = self.active_flows[i].alpha
                    maxflow = i     

    
    def FIFO_Distribution(self, EPOCH_IN_MILLIS):
        for flow in self.active_flows:
            SendRack = flow.srcID
            RecvRack = flow.dstID
            supportBps = min(self.sendBpsFree[SendRack], self.recvBpsFree[RecvRack])
            flow.currentBps = 0.0
            if supportBps > Constants.ZERO:
                if flow.parentJob.startTime>=Constants.MAXTIME:
                    flow.parentJob.startTime = self.CURRENT_TIME
                if flow.parentReducer.startTime>=Constants.MAXTIME:
                    flow.parentReducer.startTime = self.CURRENT_TIME
                if flow.startTime>=Constants.MAXTIME:
                    flow.startTime = self.CURRENT_TIME
                idealbps = (flow.remainSize)/(EPOCH_IN_MILLIS/1000)
                flow.currentBps = min(idealbps, supportBps)
                self.rackinfos[SendRack].UsedSendBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.rackinfos[RecvRack].UsedRecvBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.sendBpsFree[SendRack] = self.sendBpsFree[SendRack] - flow.currentBps
                self.recvBpsFree[RecvRack] = self.recvBpsFree[RecvRack] - flow.currentBps
    
    def MDAG_Distribution(self, EPOCH_IN_MILLIS):
        self.SortActiveFLows()
        for flow in self.active_flows:
            SendRack = flow.srcID
            RecvRack = flow.dstID
            supportBps = min(self.sendBpsFree[SendRack], self.recvBpsFree[RecvRack])
            flow.currentBps = 0.0
            if supportBps > Constants.ZERO:
                if flow.parentJob.startTime>=Constants.MAXTIME:
                    flow.parentJob.startTime = self.CURRENT_TIME
                if flow.parentReducer.startTime>=Constants.MAXTIME:
                    flow.parentReducer.startTime = self.CURRENT_TIME
                if flow.startTime>=Constants.MAXTIME:
                    flow.startTime = self.CURRENT_TIME
                idealbps = (flow.remainSize)/(EPOCH_IN_MILLIS/1000)
                flow.currentBps = min(idealbps, supportBps)
                self.rackinfos[SendRack].UsedSendBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.rackinfos[RecvRack].UsedRecvBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.sendBpsFree[SendRack] = self.sendBpsFree[SendRack] - flow.currentBps
                self.recvBpsFree[RecvRack] = self.recvBpsFree[RecvRack] - flow.currentBps

    def simulate(self, EPOCH_IN_MILLIS, saveDetail = False, debugLevel=0):
        curJob = 0
        TOTAL_JOBS = len(self.jobset.jobsList)
        while self.CURRENT_TIME<Constants.MAXTIME and (curJob<TOTAL_JOBS or self.numActiveJobs>0):
            self.active_flows = []
            self.active_Compus = []
            jobsAdded = 0
            #step1 : 添加新时间窗口的JOB，并将job和他包括的reducetask设置为submitted
            #对job进行排序
            while curJob<TOTAL_JOBS:
                job = self.jobset.jobsList[curJob]
                if job.submitTime >= self.CURRENT_TIME + EPOCH_IN_MILLIS:
                    break
                jobsAdded = jobsAdded + 1
                job.jobActive = Constants.SUBMITTED
                self.numActiveJobs = self.numActiveJobs + 1
                self.ActiveJobAdd(job)
                curJob = curJob + 1
            for ajob in self.active_jobs:
                ajob.updateExpectedTime()
                ajob.updateAlphaBeta()
            self.active_jobs.sort(key=lambda x:x.expectedTime)
            #step2 ：将active_jobs中的flows和依赖完成的compu展开，并
            for ajob in self.active_jobs:
                for rtask in ajob.reducerList:
                    if rtask.reducerActive == Constants.SUBMITTED \
                        or rtask.reducerActive == Constants.STARTED:
                        #random.shuffle(rtask.flowList)
                        
                        # SEBF 
                        temp_list = list(range(0, len(rtask.flowList)))
                        random.shuffle(temp_list)
                        for index in temp_list:
                            #print("REMAIN",rtask.flowList[index].flowName,rtask.flowList[index].remainSize)
                            if rtask.flowList[index].remainSize > Constants.ZERO:
                                self.active_flows.append(rtask.flowList[index])
                        '''
                        # SEBF + optimize
                        for flow in rtask.flowList:
                            if flow.remainSize>Constants.ZERO:
                                #flow.beta = flow.remainSize
                                self.active_flows.append(flow)
                        '''
                        for compu in rtask.compuList:
                            isready = compu.is_ready()
                            if  compu.remainSize > Constants.ZERO and isready:
                                self.active_Compus.append(compu)   
            #random.shuffle(self.active_flows)
            #step3 ：将active_flows排序，给各个active的flow安排bps，以及active的compu安排cps
            self.resetBpsCpsFree()
            if self.algorithm == "FIFO":
                self.FIFO_Distribution(EPOCH_IN_MILLIS)
            elif self.algorithm == "SEBF":
                self.SEBF_Distribution(EPOCH_IN_MILLIS)
            else:
                self.MDAG_Distribution(EPOCH_IN_MILLIS)    
            for comp in self.active_Compus:
                RackId = comp.locationID
                supportCps = self.rackCpsFree[RackId]
                comp.currentCps = 0.0
                if supportCps > Constants.ZERO:
                    if comp.parentReducer.startTime>=Constants.MAXTIME:
                        comp.parentReducer.startTime = self.CURRENT_TIME                    
                    if comp.startTime>=Constants.MAXTIME:
                        comp.startTime = self.CURRENT_TIME
                    idealcps = comp.remainSize/(EPOCH_IN_MILLIS/1000)
                    comp.currentCps = min(idealcps, supportCps)
                    self.rackinfos[RackId].UsedCpsPro[comp.compuName] = comp.currentCps/Constants.RACK_COMP_PER_SEC
                    self.rackCpsFree[RackId] = self.rackCpsFree[RackId] - comp.currentCps
            #step4 ：开始仿真结算
            for flow in self.active_flows:
                flow.remainSize = flow.remainSize - (EPOCH_IN_MILLIS/1000)*flow.currentBps
                # a flow is finished
                if flow.remainSize<Constants.ZERO and flow.finishTime>=Constants.MAXTIME:
                    flow.remainSize = 0.0
                    flow.finishTime = self.CURRENT_TIME
                    flow.parentReducer.finFlowNum += 1
                    flow.update_graph()
                    if flow.parentReducer.finFlowNum>=len(flow.parentReducer.flowList):
                        flow.parentJob.flowFinishTime = self.CURRENT_TIME
            for comp in self.active_Compus:
                comp.remainSize = comp.remainSize - (EPOCH_IN_MILLIS/1000)*comp.currentCps
                if comp.remainSize<Constants.ZERO and comp.finishTime>=Constants.MAXTIME:
                    comp.remainSize = 0.0
                    comp.finishTime = self.CURRENT_TIME
                    comp.parentReducer.finCompuNum += 1
                    if comp.parentReducer.finFlowNum>=len(comp.parentReducer.flowList)\
                        and comp.parentReducer.finCompuNum>=len(comp.parentReducer.compuList):
                        comp.parentReducer.finishTime = self.CURRENT_TIME
                        comp.parentReducer.reducerActive = Constants.FINISHED
                        comp.parentJob.finReducerNum += 1 
                        if comp.parentJob.finReducerNum>=len(comp.parentJob.reducerList):
                            comp.parentJob.finishTime = self.CURRENT_TIME
                            comp.parentJob.jobActive = Constants.FINISHED 
                            self.FinishedJobs.append(comp.parentJob.jobName)   
                            self.active_jobs.remove(comp.parentJob)   
                            self.numActiveJobs = self.numActiveJobs - 1
            
            self.debug_info(level = debugLevel)
            if saveDetail:
                self.savelog(1)
            self.CURRENT_TIME = self.CURRENT_TIME + EPOCH_IN_MILLIS
        self.logjobtime()
        print("Recv:", self.static_recv)
        print("Comp:", self.static_comp)
        print("Recv and Comp", self.static_recvcomp)
        totalnum = self.static_recv+self.static_comp - self.static_recvcomp
        if totalnum>0:
            print("Overlap(recv and comp) Rate:", self.static_recvcomp/totalnum)