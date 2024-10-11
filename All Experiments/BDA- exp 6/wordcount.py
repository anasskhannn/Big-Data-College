"""
MapReduce Word Count Sales
"""
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCount(MRJob):

    def mapper(self, _, line):
        """Mapper for WordCount"""
        row = line.strip().split(" ")
        for word in row:
            yield word , 1

    def reducer(self, key, values): 
        """Reducer for WordCount"""
        monthly_revenue = [float(amount) for amount in  values]
        yield  key, monthly_revenue
            
if __name__ == '__main__':
    WordCount.run()
