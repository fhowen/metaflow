import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
from JobSet import JobSet
from Simulator import Simulator
import sys

if __name__ == '__main__':
    js = JobSet()
    js.readTrace()
    # if 0, generate new dags
    # if 1, read from existing *.txt files 
    js.genDags(1)
    #js.storeDag()
    if len(sys.argv)>1:
        if sys.argv[1] == "MDAG" or sys.argv[1] == "SEBF" or sys.argv[1] == "FIFO":
            simu = Simulator(js,sys.argv[1])
        else:
            print("Use Default Algorithm : MDAG")
            simu = Simulator(js,"MDAG")
            
    else:
        print("Use Default Algorithm : MDAG")
        simu = Simulator(js,"MDAG")
    simu.simulate(1)