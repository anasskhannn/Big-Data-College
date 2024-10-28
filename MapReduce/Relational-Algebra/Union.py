import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class Union(MRJob):
    def mapper(self, _, line):
        yield line, None

    def reducer(self, key, _):
        yield None, key

if __name__ == "__main__":
    Union().run()

    