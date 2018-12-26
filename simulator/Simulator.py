import Constants
import Job
class Simulator:
    def __init__(self, jobset):
        self.jobset = jobset
        self.numActiveJobs = 0
        self.active_jobs = []
        self.active_flows = []
        self.active_Compus = []
        self.CURRENT_TIME = 0
        self.sendBpsFree = []
        self.recvBpsFree = []
        self.rockCpsFree = []

    def ActiveJobAdd(self, job):
        #我们这里直接按照Job的时间顺序进行插入，后面可能设计到其他的插入排序方法
        self.active_jobs.append(job)
        #对添加的job中的reducer设置为active
        for i in range(len(job.reducerList)):
            job.reducerList[i].reducerActive = Constants.SUBMITTED

    def simulate(self, EPOCH_IN_MILLIS):
        curJob = 0
        TOTAL_JOBS = len(self.jobset.jobsList)
        while self.CURRENT_TIME<Constants.MAXTIME and (curJob<TOTAL_JOBS or self.numActiveJobs>0):
            #for debug
            if self.CURRENT_TIME > 15000: break
            
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
            #step2 ：将active_jobs中的flows和依赖完成的compu展开排序
            for ajob in active_jobs:
                for rtask in ajob.reducerList:
                    if rtask.reducerActive == Constants.SUBMITTED \
                        or rtask.reducerActive == Constants.STARTED:
                        for flow in rtask.flowList:
                            if flow.remainSize * 1048576.0>Constants.ZERO:
                                flow.beta = flow.remainSize
                                self.active_flows.append(flow)
                        for compu in rtask.compuList:
                            
                        

            self.CURRENT_TIME = self.CURRENT_TIME + EPOCH_IN_MILLIS
        



        #步骤一：遍历curJob之后的jobset，将时间在CURRENT_TIME+EPOCH_IN_MILLIS之前的job设置为Active
        # 把active的job提取到active_jobs，在插入过程中排序，并且把其Reducer设置为Active
        # 将Reducer中的flow以及compute部分设置为active
        

        pass
        #步骤二：将所有active_jobs中的flow进行权值赋值
        pass
        #步骤三：将所有的流展开（注意是所有ReduceTask的要混在一起排序，三元组（j,depth,-flowsize）），
        # 计算也展开这个是直接展开不需要额外排序（并且按照先来的工作先处理原则）
        pass
        #步骤四：进行仿真前的初始化工作：
        # 将机器带宽与计算占用清空
        # 计算所有目前active的流下一时间段的bps以及计算任务下一时间端使用的资源cps
        pass
        #进行最后仿真工作，并更新流，计算的状态，并且在每个时刻之后，统计任务完成情况，并且更新分配策略