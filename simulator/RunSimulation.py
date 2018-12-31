import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
from JobSet import JobSet
from Simulator import Simulator
import sys

if __name__ == '__main__':
    if len(sys.argv)>1:
        if sys.argv[1] == "MDAG" or sys.argv[1] == "SEBF" or sys.argv[1] == "FIFO":
            js = JobSet()
            js.readTrace()
            js.genDag()
            simu = Simulator(js,sys.argv[1])
            simu.simulate(1)
        else:
            print("Use Default Algorithm : MDAG")
            js = JobSet()
            js.readTrace()
            js.genDag()
            simu = Simulator(js,"MDAG")
            simu.simulate(1)
    else:
        print("Use Default Algorithm : MDAG")
        js = JobSet()
        js.readTrace()
        js.genDag()
        simu = Simulator(js,"MDAG")
        simu.simulate(1)