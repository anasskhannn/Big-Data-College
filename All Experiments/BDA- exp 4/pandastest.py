from mrjob.job import MRJob
import pandas as pd

class MRPandasJob(MRJob):

    def mapper_init(self):
        # Load data into Pandas DataFrame in the mapper_init method
        self.df = pd.read_csv('/home/nooman/Downloads/Ecommerce/Orders.csv')

    def mapper(self, _, line):
        # Perform operations using Pandas DataFrame
        # Example: Count occurrences of each value in a column
        counts = self.df['CustomerID'].value_counts()
        # Emit results
        for value, count in counts.items():
            yield value, count

    def reducer(self, key, values):
        # Sum the counts for each key
        yield key, sum(values)

if __name__ == '__main__':
    MRPandasJob.run()
