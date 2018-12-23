import Constants

class Compu:
    TotalCompuNum = 0

    def __init__(self, compu_name, parent_reducer):
        self.compuName = compu_name
        self.parentReducer = parent_reducer
        self.compuID = Compu.TotalCompuNum
        Compu.TotalCompuNum += 1
    
    def set_attributes(self, location_id, compu_size):
        self.locationID = location_id
        self.compuSize = compu_size
        self.startTime = Constants.MAXTIME
        self.finishTime = Constants.MAXTIME
        self.remainSize = compu_size
        self.currentCps = 0
