import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class Intersection(MRJob):
    def mapper(self, _, line):
        yield line, 1

    def reducer(self, key, values):
        count = sum(values)
        if count == 2:
            yield count, key  

if __name__ == "__main__":
    Intersection().run()