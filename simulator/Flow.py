import Constants

class Flow:
    __slots__ = ['flowName', 'flowID', 'parentReducer', 'parentJob', 'alpha', \
                 'beta', 'startTime', 'srcID', 'dstID', 'flowSize', 'submitTime', \
                 'finishTime', 'currentBps', 'remainSize','metaflowTag']
    
    TotalFlowNum = 0
    def __init__(self, flow_name, parent_reducer):
        self.flowName = flow_name
        self.flowID = Flow.TotalFlowNum
        self.parentReducer = parent_reducer
        self.parentJob = self.parentReducer.parentJob
        # max number of compu tasks from this flow node to end
        self.alpha = 0
        self.beta = 0
        self.startTime = Constants.MAXTIME
        Flow.TotalFlowNum += 1
        
    def set_attributes(self, src_id, dst_id, flow_size, submit_time):
        self.srcID = src_id
        self.dstID = dst_id
        self.flowSize = flow_size
        self.submitTime = submit_time
        self.finishTime = Constants.MAXTIME
        self.currentBps = 0
        self.remainSize = flow_size
    
    def update_remain_size(self, new_sent):
        self.remainSize = self.remainSize - new_sent

    def update_graph(self):
        for jtask in self.parentReducer.compuList:
            if self.flowID in jtask.neededFlow:
                jtask.neededFlow.remove(self.flowID)
    


