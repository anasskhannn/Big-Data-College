import mrjob 
from mrjob.job import MRJob
from mrjob.step import MRStep


class Selection(MRJob):
    def mapper(self, _, line):
        row = line.strip().split(",")
        country = row[6]
        if country == "India":
            yield None, line
        # row = line.strip().split(",")
        # # country = row[6]
        # # if country == "India":
        #     yield country, 1

    # def reducer(self, key, value):
    #     yield key, sum(value)

if __name__ == "__main__":
    Selection().run()
