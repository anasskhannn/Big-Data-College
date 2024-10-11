import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MonthlyCustomerCount(MRJob):
    def mapper(self, _, line):
        rows = line.split(",")
        custoerjoiningdate = rows[-1][:10]
        custoerjoiningdate = datetime.strptime(custoerjoiningdate, '%Y-%m-%d')
        month = custoerjoiningdate.month
        yield month, 1
        
    def reducer(self, key, value):
        yield key, sum(value)

if __name__ == "__main__":
    MonthlyCustomerCount.run()