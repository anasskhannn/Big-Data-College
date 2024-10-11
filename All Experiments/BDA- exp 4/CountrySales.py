import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep


# Country sales 
class CountrySum(MRJob):
    table1 = "Customers"
    table2 = "Orders"

    def mapper(self, _, line):
        row  = line.strip().split(',')
        if len(row)==11:
            yield row[0], (row, CountrySum.table1)
        else:
            yield row[1], (row, CountrySum.table2)

    def reducer(self, key, values):
        customers = []
        orders = []
        for value, table in values:
            if table == CountrySum.table1:
                customers.append(value)
            elif table == CountrySum.table2:
                orders.append(value)

        if customers and orders:
            for cus in customers:
                for order in orders:
                    if cus[0] == order[1]:
                        yield cus[6], order[-1]


    # def combiner2(self, key, values):
    #     yield key, sum(float(s) for s in values)


    def reducer2(self, key, values):
        total_sum = sum(float(s) for s in values)
        yield None, (round(total_sum), key)

    def reducer3(self, key, values):
        sorted_sum = sorted(values, reverse = True)
        for value, country in sorted_sum:
            yield country, value

    def steps(self):
        return [
            MRStep(mapper = self.mapper, reducer = self.reducer),
            MRStep(reducer = self.reducer2),
            MRStep(reducer = self.reducer3)
        ]
    
        
if __name__ == '__main__':
    CountrySum.run()


        

