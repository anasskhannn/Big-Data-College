from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

# India YM Sales 
class MonthlySales(MRJob):
    table1 = "Customers"
    table2 = "Orders"
    def mapper(self, _ , line):
        row = line.strip().split(",")
        if len(row) == 11:
            yield row[0] , (row, MonthlySales.table1)
        elif len(row) == 8:
            yield row[1], (row, MonthlySales.table2)


    def reducer(self, key, values):
        customers = []
        orders = []

        for data, table in values:
            if table == MonthlySales.table1:
                customers.append(data)
            else:
                orders.append(data)

        if customers and orders:
            for customer in customers:
                for order in orders:
                    if customer[6] == "India":
                        yield datetime.strptime(order[3], "%d-%m-%Y").year,(datetime.strptime(order[3], "%d-%m-%Y").month , float(order[-1]))

    def mapper2(self, key, value):
        monthly_sales = {}
        for month, amount in value:
            if month in monthly_sales:
                monthly_sales[month] += amount
            else:
                monthly_sales[month] = amount

        monthly_sales = sorted(monthly_sales.items())
        for m, s in monthly_sales:
            yield key, (m,round(s)) 

    def steps(self):
        return [
            MRStep(mapper = self.mapper, reducer = self.reducer),
            MRStep(reducer = self.mapper2)
        ]

if __name__ == "__main__":
    MonthlySales.run()
          

