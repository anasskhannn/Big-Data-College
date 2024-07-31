import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

"""Wordd Count problem"""
# class WordCount(MRJob):
#     def mapper(self, _, line):
#         words = line.strip().split(",")
#         for word in words:
#             yield word, 1

#     def reducer(self,key,values):
#         yield key, sum(values)


# if __name__ == "__main__":
#     WordCount.run()


"""Monthly Sales data"""

from datetime import datetime

class MonthlySales(MRJob):
    def mapper(self, _, line):
        row = line.strip().split(",")

        # date is in 4rth col
        orderdate= row[3]
        # convert orderdate(text) to datetime format
        month = datetime.strptime(orderdate,'%d-%m-%Y').month
        # amount is in last col
        amount= row[-1]
        yield month, amount


    def reducer(self, key, value):
        yield key, sum([float(amount) for amount in value])

if __name__ == "__main__":
    MonthlySales.run()