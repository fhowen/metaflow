import Constants

class Flow:

    def __init__(self, flow_name, flow_id):
        self.flowName = flow_name
        self.flowID = flow_id
        
    def set_attributes(self, src_id, dst_id, flow_size, start_time):
        self.srcID = src_id
        self.dstID = dst_id
        self.flowSize = flow_size
        self.startTime = start_time
        self.finishTime = Constants.MAXTIME
        self.currentBps = 0
        self.remainSize = flow_size
    
    def update_remain_size(self, new_sent):
        self.remainSize = self.remainSize - new_sent


