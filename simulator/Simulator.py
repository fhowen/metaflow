import Constants
import Job
class Simulator:
    def __init__(self, jobset):
        self.jobset = jobset
        self.numActiveJobs = 0
        self.active_jobs = []
        self.active_flows = []
        self.active_Compus = []
        self.sendBpsFree = []
        self.recvBpsFree = []
        self.rockCpsFree = []
        self.CURRENT_TIME = 0
        for i in range(Constants.MACHINENUM):
            self.sendBpsFree.append(Constants.RACK_BITS_PER_SEC)
            self.recvBpsFree.append(Constants.RACK_BITS_PER_SEC)
            self.rockCpsFree.append(Constants.RACK_COMP_PER_SEC)

    def ActiveJobAdd(self, job):
        #我们这里直接按照Job的时间顺序进行插入，后面可能设计到其他的插入排序方法
        self.active_jobs.append(job)
        #对添加的job中的reducer设置为active
        for i in range(len(job.reducerList)):
            job.reducerList[i].reducerActive = Constants.SUBMITTED
    
    def SortActiveFLows(self):
        self.active_flows.sort(key=lambda x:(self.active_jobs.index(x.parentJob),-x.alpha,x.beta))
    
    def resetBpsCpsFree(self):
        for i in range(Constants.MACHINENUM):
            self.sendBpsFree[i] = Constants.RACK_BITS_PER_SEC
            self.recvBpsFree[i] = Constants.RACK_BITS_PER_SEC
            self.rockCpsFree[i] = Constants.RACK_COMP_PER_SEC


    def simulate(self, EPOCH_IN_MILLIS):
        curJob = 0
        TOTAL_JOBS = len(self.jobset.jobsList)
        while self.CURRENT_TIME<Constants.MAXTIME and (curJob<TOTAL_JOBS or self.numActiveJobs>0):
            self.active_flows = []
            self.active_Compus = []
            jobsAdded = 0
            #step1 : 添加新时间窗口的JOB，并将job和他包括的reducetask设置为submitted
            while curJob<TOTAL_JOBS:
                job = self.jobset.jobsList[curJob]
                if job.submitTime > self.CURRENT_TIME + EPOCH_IN_MILLIS:
                    break
                jobsAdded = jobsAdded + 1
                job.jobActive = Constants.SUBMITTED
                self.numActiveJobs = self.numActiveJobs + 1
                self.ActiveJobAdd(job)
                curJob = curJob + 1
            #step2 ：将active_jobs中的flows和依赖完成的compu展开，并将active_flows排序
            for ajob in self.active_jobs:
                for rtask in ajob.reducerList:
                    if rtask.reducerActive == Constants.SUBMITTED \
                        or rtask.reducerActive == Constants.STARTED:
                        for flow in rtask.flowList:
                            if flow.remainSize * 1048576.0>Constants.ZERO:
                                flow.beta = flow.remainSize
                                self.active_flows.append(flow)
                        for compu in rtask.compuList:
                            isready = compu.is_ready()
                            if  compu.remainSize > Constants.ZERO and isready:
                                self.active_Compus.append(compu)
            self.SortActiveFLows()
            #step3 ：给各个active的flow安排bps，以及active的compu安排cps
            self.resetBpsCpsFree()
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
                    idealbps = (8*1048576*flow.remainSize)/(EPOCH_IN_MILLIS/1000)
                    flow.currentBps = min(idealbps, supportBps)
                    self.sendBpsFree[SendRack] = self.sendBpsFree[SendRack] - flow.currentBps
                    self.recvBpsFree[RecvRack] = self.recvBpsFree[RecvRack] - flow.currentBps
            for comp in self.active_Compus:
                RackId = comp.compuID
                supportCps = self.rockCpsFree[RackId]
                comp.currentCps = 0.0
                if supportCps > Constants.ZERO:
                    if comp.parentReducer.startTime>=Constants.MAXTIME:
                        comp.parentReducer.startTime = self.CURRENT_TIME                    
                    if comp.startTime>=Constants.MAXTIME:
                        comp.startTime = self.CURRENT_TIME
                    idealcps = comp.remainSize/(EPOCH_IN_MILLIS/1000)
                    comp.currentCps = min(idealcps, supportCps)
                    self.rockCpsFree[RackId] = self.rockCpsFree[RackId] - comp.currentCps
            #step4 ：开始仿真结算
            for flow in self.active_flows:
                flow.remainSize = flow.remainSize - (EPOCH_IN_MILLIS/1000)*flow.currentBps/(1048576*8)
                if flow.remainSize<Constants.ZERO and flow.finishTime>=Constants.MAXTIME:
                    flow.remainSize = 0.0
                    flow.finishTime = self.CURRENT_TIME
                    flow.parentReducer.finFlowNum += 1
                    if flow.parentReducer.finFlowNum>=len(flow.parentReducer.flowList):
                        flow.parentJob.flowFinishTime = self.CURRENT_TIME
                    if flow.parentReducer.finFlowNum>=len(flow.parentReducer.flowList)\
                        and flow.parentReducer.finCompuNum>=len(flow.parentReducer.compuList):
                        flow.parentReducer.finishTime = self.CURRENT_TIME
                        flow.parentReducer.reducerActive = Constants.FINISHED
                        flow.parentJob.finReducerNum += 1
                        if flow.parentJob.finReducerNum>=len(flow.parentJob.reducerList):
                            flow.parentJob.finishTime = self.CURRENT_TIME
                            flow.parentJob.jobActive = Constants.FINISHED
                            self.active_jobs.remove(flow.parentJob)
                            self.numActiveJobs = self.numActiveJobs - 1
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
                            self.active_jobs.remove(comp.parentJob)   
                            self.numActiveJobs = self.numActiveJobs - 1                                                    
            self.CURRENT_TIME = self.CURRENT_TIME + EPOCH_IN_MILLIS
        