class A:
    def __init__(self, parent_task):
        self.parentTask = parent_task


class B:
    def __init__(self, tag):
        self.tag = tag
        
    def add_A(self):
        self.A = A(self)


b= B(3)
b.add_A()
print(b)
print(b.A.parentTask)
b.A.parentTask.tag = 999
print(b.tag)