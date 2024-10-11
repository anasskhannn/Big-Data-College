import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class Difference(MRJob):

    def mapper(self, _, line):
        yield line, 1

    def reducer(self, key, value):
        count = sum(value)
        # if count == 1 and key.split()[-1] == 'B':
        yield None, key

if __name__ == "__main__":
    Difference().run()