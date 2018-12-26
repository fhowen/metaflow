import Constants
from Flow import Flow
from Compu import Compu
from Reducer import Reducer
from Job import Job
from JobSet import JobSet


js = JobSet()
js.readTrace()
print(Flow.TotalFlowNum)
js.genDag()
print(Flow.TotalFlowNum)
test_c = js.jobsList[0].reducerList[0].compuList[0]
print(test_c.is_ready())