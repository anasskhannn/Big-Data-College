from mrjob.job import MRJob
from mrjob.step import MRStep

class Selection(MRJob):
    def mapper(self, _, line):
        row = line.strip().split(',')
        yield None, (row[1], row[2], row[3], row[6])
    
if __name__ == "__main__":
    Selection().run()