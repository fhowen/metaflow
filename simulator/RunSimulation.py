import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
from JobSet import JobSet
from Simulator import Simulator


js = JobSet()
js.readTrace()
#print(Flow.TotalFlowNum)
js.genDag()
#print(Flow.TotalFlowNum)
simu = Simulator(js)
simu.simulate(1000)
