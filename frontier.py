from queue import PriorityQueue
import json
import time

class Frontier:
    def __init__(self):
        self.picke_file_name = "frontier.json"
        self.q = PriorityQueue()
        self.return_count = 500

    def add(self, url, level):
        if self.size() < 90000:
            self.q.put((level, url))

    def get(self):
        return_count = self.size()
        if return_count > self.return_count:
            return_count = self.return_count
            self.return_count += (500)
        # if return_count > 5000:
        #     return_count = 5000
        if self.q:
            return_list = []
            for i in range(0,return_count):
                return_list.append(self.q.get())
            return return_list
        else:
            return None
    
    def size(self):
        return self.q.qsize()

    def save(self):
        save_list = []
        while not self.q.empty():
            save_list.append(self.q.get())
        save_dict = {}
        save_dict["q_list"] = save_list
        save_dict["return_count"] = self.return_count
        with open(self.picke_file_name, 'w') as outfile:
            json.dump(save_dict, outfile)
        # Restoring as we popped
        self.restore()
    
    def restore(self):
        with open(self.picke_file_name) as json_file:
            save_dict = json.load(json_file)
        q_list = save_dict["q_list"]
        return_count = save_dict["return_count"]
        for level, url in q_list:
            self.q.put((level, url))
        self.return_count = return_count
        print("Restored Frontier")


        