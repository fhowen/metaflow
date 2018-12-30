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
#print(js.jobsList[0].reducerList[0].flowList[0].srcID,js.jobsList[0].reducerList[0].flowList[0].dstID)
#print(js.jobsList[0].reducerList[0].flowList[0].flowSize)
#print(js.jobsList[0].reducerList[0].flowList[1].srcID,js.jobsList[0].reducerList[0].flowList[1].dstID)
#print(js.jobsList[0].reducerList[0].flowList[1].flowSize)
#print(js.jobsList[0].reducerList[1].flowList[0].srcID,js.jobsList[0].reducerList[1].flowList[0].dstID)
#print(js.jobsList[0].reducerList[1].flowList[0].flowSize)
#print(js.jobsList[0].reducerList[1].flowList[1].srcID,js.jobsList[0].reducerList[1].flowList[1].dstID)
#print(js.jobsList[0].reducerList[1].flowList[1].flowSize)
#print(len(js.jobsList[0].reducerList[0].compuList))

#print(Flow.TotalFlowNum)
simu = Simulator(js)
simu.simulate(1)
