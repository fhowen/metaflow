from igraph import *

class Cdag:
    totalCdagNum = 0

    def __init__(self):
        self.dag = Graph()
        Cdag.totalCdagNum += 1



test1 = Cdag()
test2 = Cdag()
print("Total Cdag number is %d" % Cdag.totalCdagNum)
