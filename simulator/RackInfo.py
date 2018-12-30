class RackInfo:
    __slots__ = ['UsedSendBpsPro', 'UsedRecvBpsPro', 'UsedCpsPro']
    def __init__(self):
        self.UsedSendBpsPro = {}
        self.UsedRecvBpsPro = {}
        self.UsedCpsPro = {}
    def resetinfo(self):
        self.UsedSendBpsPro.clear()
        self.UsedRecvBpsPro.clear()
        self.UsedCpsPro.clear() 
