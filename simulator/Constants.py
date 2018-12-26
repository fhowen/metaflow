import sys

MAXTIME = sys.maxsize
MACHINENUM = 150
JOBNUM = 3
ZERO = 0.001


# dag tye strings
DNNDAG = "DNN"
RANDOMDAG = "RANDOM"

# task status used for isActive
UNSUBMITTED = 0
SUBMITTED = 1
STARTED = 2
FINISHED = 3