import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job





js = JobSet()
js.readTrace()
print(Flow.TotalFlowNum)
js.readDag()
print(Flow.TotalFlowNum)