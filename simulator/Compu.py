import Constants

class Compu:
    def __init__(self, compu_name, compu_id):
        self.compuName = compu_name
        self.compuID = compu_id
    
    def set_attributes(self, location_id, compu_size):
        self.locationID = location_id
        self.compuSize = compu_size
        self.startTime = Constants.MAXTIME
        self.finishTime = Constants.MAXTIME
        self.remainSize = compu_size
        self.currentCps = 0
        