import mrjob

from mrjob.job import MRJob
from mrjob.step import MRStep


class Selection(MRJob):

    # This is Similar to Sql(Select * From Table)
    def mapper(self, _, line):
        row = line.strip().split(",")
        country=row[6] # Our Data has Country at Index 6

# We need only india Data
        if country == "India":
            yield row[0], row[1:]


    def reducer(self,key,value):
        yield key,sum(value)

if __name__ == "main":
    Selection.run()
