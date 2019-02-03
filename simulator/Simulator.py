import Constants
import Job
from RackInfo import RackInfo
import time
import os
import random
class Simulator:
    def __init__(self, jobset, algorithm="MDAG"):
        self.working_statics = []
        self.ideal_statics = []
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
        # Insert jobsa into active_jobs in chronological order
        self.active_jobs.append(job)
        # set the reducer to submitted
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
        if level>=0:
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
        working_num = 0
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
                    working_num += 1
                if rackinfo.UsedCpsPro and rackinfo.UsedRecvBpsPro:
                    self.static_recvcomp += 1
            count += 1  
        self.working_statics.append(working_num) 
        if level>=0: 
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

    def logmachrate(self):
        logfile = "logmachrate-" + self.algorithm + ".csv"
        logpath = os.path.join(Constants.LOGDIR,logfile)
        f = open(logpath, 'w')
        totaltime = len(self.ideal_statics)
        for i in range(totaltime):
            f.write(str(self.ideal_statics[i])+",")
        f.write("\n")
        for i in range(totaltime):
            f.write(str(self.working_statics[i])+",") 

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

    def VARYS_Distribution(self, EPOCH_IN_MILLIS):
        result_flows = []
        jobCoflows = {}
        for flow in self.active_flows:
            jobid = flow.parentJob.jobID
            if jobid in jobCoflows.keys():
                jobCoflows[jobid].append(flow)
            else:
                jobCoflows[jobid] = []
                jobCoflows[jobid].append(flow)
        jids = list(jobCoflows.keys())
        jids.sort()
        for jid in jids:
            sender_machine = {}
            receiver_machine = {}
            #step1:divide flows according to machines
            for f in jobCoflows[jid]:
                sid = f.srcID
                rid = f.dstID
                if sid in sender_machine.keys():
                    sender_machine[sid].append(f)
                else:
                    sender_machine[sid] = []
                    sender_machine[sid].append(f)
                if rid in receiver_machine.keys():
                    receiver_machine[rid].append(f)
                else:
                    receiver_machine[rid] = []
                    receiver_machine[rid].append(f)  
            #step2:calculate max time                
            maxtime = -1
            for sendid in sender_machine.keys():
                remain_bps = self.sendBpsFree[sendid]
                if remain_bps < Constants.ZERO:
                    maxtime = Constants.MAXTIME
                    break
                mach_fsize = 0.0
                for sflow in sender_machine[sendid]:
                    mach_fsize += sflow.remainSize
                if maxtime < mach_fsize/remain_bps:
                    maxtime = mach_fsize/remain_bps
            for recvid in receiver_machine.keys():
                remain_bps = self.recvBpsFree[recvid]
                if remain_bps < Constants.ZERO or maxtime == Constants.MAXTIME:
                    maxtime = Constants.MAXTIME
                    break
                mach_fsize = 0.0
                for rflow in receiver_machine[recvid]:
                    mach_fsize += rflow.remainSize
                if maxtime < mach_fsize/remain_bps:
                    maxtime = mach_fsize/remain_bps
            # step3:allocate bandwidth value 
            if maxtime == Constants.MAXTIME:
                continue
            sendflow = []
            for sendid in sender_machine.keys():
                mach_fsize = 0.0
                for sflow in sender_machine[sendid]:
                    mach_fsize += sflow.remainSize
                machine_bps = mach_fsize/maxtime
                flownum = len(sender_machine[sendid])
                for sflow in sender_machine[sendid]:
                    sflow.currentBps = machine_bps/flownum
                    sendflow.append(sflow)
            recvflow = []
            for recvid in receiver_machine.keys():
                mach_fsize = 0.0
                for rflow in receiver_machine[recvid]:
                    mach_fsize += rflow.remainSize
                machine_bps = mach_fsize/maxtime
                flownum = len(receiver_machine[recvid])
                for rflow in receiver_machine[recvid]:
                    rflow.currentBps = machine_bps/flownum
                    recvflow.append(rflow)
            sendflow.sort(key=lambda x:x.flowID)
            recvflow.sort(key=lambda x:x.flowID)
            totalflownum = len(sendflow)
            for i in range(totalflownum):
                if sendflow[i].currentBps > recvflow[i].currentBps:
                    result_flows.append(recvflow[i])
                else:
                    result_flows.append(sendflow[i])
                curflow = result_flows[-1]
                self.sendBpsFree[curflow.srcID] -= curflow.currentBps
                self.recvBpsFree[curflow.dstID] -= curflow.currentBps
                self.rackinfos[curflow.srcID].UsedSendBpsPro[curflow.flowName] \
                            = curflow.currentBps/Constants.RACK_BITS_PER_SEC
                self.rackinfos[curflow.dstID].UsedRecvBpsPro[curflow.flowName] \
                            = curflow.currentBps/Constants.RACK_BITS_PER_SEC
                if curflow.parentJob.startTime>=Constants.MAXTIME:
                    curflow.parentJob.startTime = self.CURRENT_TIME
                if curflow.parentReducer.startTime>=Constants.MAXTIME:
                    curflow.parentReducer.startTime = self.CURRENT_TIME
                if curflow.startTime>=Constants.MAXTIME:
                    curflow.startTime = self.CURRENT_TIME 
        # work conservation
        for flow in result_flows:
            SendRack = flow.srcID
            RecvRack = flow.dstID
            supportBps = min(self.sendBpsFree[SendRack], self.recvBpsFree[RecvRack])
            if supportBps > Constants.ZERO:
                if flow.parentJob.startTime>=Constants.MAXTIME:
                    flow.parentJob.startTime = self.CURRENT_TIME
                if flow.parentReducer.startTime>=Constants.MAXTIME:
                    flow.parentReducer.startTime = self.CURRENT_TIME
                if flow.startTime>=Constants.MAXTIME:
                    flow.startTime = self.CURRENT_TIME
                idealbps = (flow.remainSize-flow.currentBps*(EPOCH_IN_MILLIS/1000))/(EPOCH_IN_MILLIS/1000)
                flow.currentBps += min(idealbps, supportBps)
                self.rackinfos[SendRack].UsedSendBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.rackinfos[RecvRack].UsedRecvBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                self.sendBpsFree[SendRack] = self.sendBpsFree[SendRack] - min(idealbps, supportBps)
                self.recvBpsFree[RecvRack] = self.recvBpsFree[RecvRack] - min(idealbps, supportBps) 
        self.active_flows = result_flows
                        
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
                
    def MDAG_Distribution(self, EPOCH_IN_MILLIS, MDDA = True):
        result_flows = []
        jobMFs = {}
        for flow in self.active_flows:
            jobid = flow.parentJob.jobID
            mftag = flow.metaflowTag
            if jobid in jobMFs.keys():
                if mftag in jobMFs[jobid].keys():    
                    jobMFs[jobid][mftag].append(flow)
                else:
                    jobMFs[jobid][mftag] = []
                    jobMFs[jobid][mftag].append(flow)
            else:
                jobMFs[jobid] = {}
                jobMFs[jobid][mftag] = []
                jobMFs[jobid][mftag].append(flow)
        jids = list(jobMFs.keys())
        for jid in jids:
            mf_ready = []
            mf_non_ready = []
            for mf_tag in jobMFs[jid].keys():
                isReady = True
                for f in jobMFs[jid][mf_tag]:
                    if f.alpha == 0:
                        isReady = False
                        break
                if isReady:
                    mf_ready.append(jobMFs[jid][mf_tag])
                else:
                    mf_non_ready.append(jobMFs[jid][mf_tag])
            # sort mf_ready and mf_non_ready
            mf_ready.sort(key=lambda x: -sum([item.beta for item in x]))
            mf_non_ready.sort(key=lambda x: -sum([item.beta for item in x]))
            # merge mf_ready and mf_non_ready
            mf_list = mf_ready + mf_non_ready
            # allocate bandwidth for mf_ready and mf_non_ready
            for mf in mf_list:
                # allocate bandwidth for mf
                # METHOD1: no MDDA
                if MDDA == False:
                    for flow in mf:
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
                            result_flows.append(flow)
                # METHOD2: MDDA
                else:
                    # step1: divide flows in MF according to machines(receiver and sender)
                    sender_machine = {}
                    receiver_machine = {}
                    for f in mf:
                        sid = f.srcID
                        rid = f.dstID
                        if sid in sender_machine.keys():
                            sender_machine[sid].append(f)
                        else:
                            sender_machine[sid] = []
                            sender_machine[sid].append(f)
                        if rid in receiver_machine.keys():
                            receiver_machine[rid].append(f)
                        else:
                            receiver_machine[rid] = []
                            receiver_machine[rid].append(f)                    
                    # step2: calculate slowest time according to machines and remainsize
                    maxtime = -1
                    for sendid in sender_machine.keys():
                        remain_bps = self.sendBpsFree[sendid]
                        if remain_bps < Constants.ZERO:
                            maxtime = Constants.MAXTIME
                            break
                        mach_fsize = 0.0
                        for sflow in sender_machine[sendid]:
                            mach_fsize += sflow.remainSize
                        if maxtime < mach_fsize/remain_bps:
                            maxtime = mach_fsize/remain_bps
                    for recvid in receiver_machine.keys():
                        remain_bps = self.recvBpsFree[recvid]
                        if remain_bps < Constants.ZERO or maxtime == Constants.MAXTIME:
                            maxtime = Constants.MAXTIME
                            break
                        mach_fsize = 0.0
                        for rflow in receiver_machine[recvid]:
                            mach_fsize += rflow.remainSize
                        if maxtime < mach_fsize/remain_bps:
                            maxtime = mach_fsize/remain_bps
                    # step3: allocate bandwidth value = max(remain, flowsize/slowest time)
                    if maxtime == Constants.MAXTIME:
                        continue
                    sendflow = []
                    for sendid in sender_machine.keys():
                        mach_fsize = 0.0
                        for sflow in sender_machine[sendid]:
                            mach_fsize += sflow.remainSize
                        machine_bps = mach_fsize/maxtime
                        flownum = len(sender_machine[sendid])
                        for sflow in sender_machine[sendid]:
                            sflow.currentBps = machine_bps/flownum
                            sendflow.append(sflow)

                    recvflow = []
                    for recvid in receiver_machine.keys():
                        mach_fsize = 0.0
                        for rflow in receiver_machine[recvid]:
                            mach_fsize += rflow.remainSize
                        machine_bps = mach_fsize/maxtime
                        flownum = len(receiver_machine[recvid])
                        for rflow in receiver_machine[recvid]:
                            recvflow.append(rflow)
                    sendflow.sort(key=lambda x:x.flowID)
                    recvflow.sort(key=lambda x:x.flowID)
                    mflownum = len(sendflow)
                    for i in range(mflownum):
                        if sendflow[i].currentBps > recvflow[i].currentBps:
                            result_flows.append(recvflow[i])
                        else:
                            result_flows.append(sendflow[i])
                        rflow = result_flows[-1]
                        self.sendBpsFree[rflow.srcID] -= rflow.currentBps
                        self.recvBpsFree[rflow.dstID] -= rflow.currentBps
                        self.rackinfos[rflow.srcID].UsedSendBpsPro[rflow.flowName] \
                                = rflow.currentBps/Constants.RACK_BITS_PER_SEC
                        self.rackinfos[rflow.dstID].UsedRecvBpsPro[rflow.flowName] \
                                = rflow.currentBps/Constants.RACK_BITS_PER_SEC
                        if rflow.parentJob.startTime>=Constants.MAXTIME:
                            rflow.parentJob.startTime = self.CURRENT_TIME
                        if rflow.parentReducer.startTime>=Constants.MAXTIME:
                            rflow.parentReducer.startTime = self.CURRENT_TIME
                        if rflow.startTime>=Constants.MAXTIME:
                            rflow.startTime = self.CURRENT_TIME 
        #work conservation  
        if MDDA == True:
            result_flows.sort(key=lambda x:(self.active_jobs.index(x.parentJob),-x.alpha,-x.beta))
            for flow in result_flows:
                SendRack = flow.srcID
                RecvRack = flow.dstID
                supportBps = min(self.sendBpsFree[SendRack], self.recvBpsFree[RecvRack])
                if supportBps > Constants.ZERO:
                    if flow.parentJob.startTime>=Constants.MAXTIME:
                        flow.parentJob.startTime = self.CURRENT_TIME
                    if flow.parentReducer.startTime>=Constants.MAXTIME:
                        flow.parentReducer.startTime = self.CURRENT_TIME
                    if flow.startTime>=Constants.MAXTIME:
                        flow.startTime = self.CURRENT_TIME
                    idealbps = (flow.remainSize-flow.currentBps*(EPOCH_IN_MILLIS/1000))/(EPOCH_IN_MILLIS/1000)
                    flow.currentBps += min(idealbps, supportBps)
                    self.rackinfos[SendRack].UsedSendBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                    self.rackinfos[RecvRack].UsedRecvBpsPro[flow.flowName] = flow.currentBps/Constants.RACK_BITS_PER_SEC
                    self.sendBpsFree[SendRack] = self.sendBpsFree[SendRack] - min(idealbps, supportBps)
                    self.recvBpsFree[RecvRack] = self.recvBpsFree[RecvRack] - min(idealbps, supportBps)
        self.active_flows = result_flows 
        
    def simulate(self, EPOCH_IN_MILLIS, saveDetail = False, debugLevel=1):
        curJob = 0
        TOTAL_JOBS = len(self.jobset.jobsList)
        while self.CURRENT_TIME<Constants.MAXTIME and (curJob<TOTAL_JOBS or self.numActiveJobs>0):
            self.active_flows = []
            self.active_Compus = []
            jobsAdded = 0
            #Step1: Add new jobs, Update the attitudes and Sort jobs
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
            if self.algorithm == "MDAG":
                self.active_jobs.sort(key=lambda x:x.expectedTime)
            #Step2: Extract flows and computes from jobs (+ calculate ideal_statics)
            ideal_num = 0
            working_mach = {}
            for ajob in self.active_jobs:
                for rtask in ajob.reducerList:
                    if rtask.reducerActive == Constants.SUBMITTED \
                        or rtask.reducerActive == Constants.STARTED:
                        temp_list = list(range(0, len(rtask.flowList)))
                        #random.shuffle(temp_list) # random the flow
                        for index in temp_list:
                            if rtask.flowList[index].remainSize > Constants.ZERO:
                                self.active_flows.append(rtask.flowList[index])
                        for compu in rtask.compuList:
                            isready = compu.is_ready()
                            if  compu.remainSize > Constants.ZERO:
                                if compu.locationID not in working_mach.keys():
                                    ideal_num += 1
                                    working_mach[compu.locationID] = True
                                if isready:
                                    self.active_Compus.append(compu) 
            self.ideal_statics.append(ideal_num)
            #Step3: Allocate bandwidth and processors
            self.resetBpsCpsFree()
            if self.algorithm == "FIFO":
                self.FIFO_Distribution(EPOCH_IN_MILLIS)
            elif self.algorithm == "VARYS":
                self.VARYS_Distribution(EPOCH_IN_MILLIS)
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
            #Step4: Start simulation including network and computation 
            for flow in self.active_flows:
                flow.remainSize = flow.remainSize - (EPOCH_IN_MILLIS/1000)*flow.currentBps
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
            #Step5: Statistics
            self.debug_info(level = debugLevel)
            if saveDetail:
                self.savelog(1)
            self.CURRENT_TIME = self.CURRENT_TIME + EPOCH_IN_MILLIS
        self.logjobtime()
        self.logmachrate()
        print("Recv:", self.static_recv)
        print("Comp:", self.static_comp)
        print("Recv and Comp", self.static_recvcomp)
        totalnum = self.static_recv+self.static_comp - self.static_recvcomp
        if totalnum>0:
            print("Overlap(recv and comp) Rate:", self.static_recvcomp/totalnum)