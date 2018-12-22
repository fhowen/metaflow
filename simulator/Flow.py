class Flow:

    def __init__(self, src_id, dst_id, size, start_time):
        self.src_id = src_id
        self.dst_id = dst_id
        self.size = size
        self.start_time = start_time
        self.remain_size = self.size
