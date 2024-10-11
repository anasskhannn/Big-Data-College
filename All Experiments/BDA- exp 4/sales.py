import mrjob
from mrjob.job import MRJob

class Sales(MRJob):
    def mapper(self, _, line):
        row = line.strip().split(',')
        yield row[0], row[1]


    def reducer(self, key, values):
        yield key, sum([int(amount) for amount in values])


if __name__ == "__main__":
    Sales.run()        
        

