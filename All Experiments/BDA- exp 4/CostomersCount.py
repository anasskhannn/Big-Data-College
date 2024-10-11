from mrjob.job import MRJob
import logging
from mrjob.step import MRStep

## this class going to count number of customers from each country

class CountryCustomerCOunts(MRJob):
    def mapper(self, _, line):
    
        rows = line.split(',')
        country = rows[6].strip()
        yield country, 1

    def reducer(self, country, counts):
        # Sum up the counts for each word
        yield None,  (sum(counts), country)

    def reducer_sort(self, _, country_counts):
        sorted_country = sorted(country_counts, reverse = True)
        for count, country in sorted_country:
            yield country, count

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper, reducer = self.reducer
            ),
            MRStep(reducer = self.reducer_sort)
        ]
if __name__ == '__main__':
    CountryCustomerCOunts.run()
