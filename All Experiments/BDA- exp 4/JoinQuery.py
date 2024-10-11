import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep


# Monthly sales from India
class Join(MRJob):

    table1 = "Customers"
    table2 = "Orders"

    def mapper(self, _, line):
        row  = line.strip().split(',')
        if len(row)==11:
            yield row[0], (row, Join.table1)
        else:
            yield row[1], (row, Join.table2)

    def reducer(self, key, values):
        customers = []
        orders = []
        for value, table in values:
            if table == Join.table1:
                customers.append(value)
            elif table == Join.table2:
                orders.append(value)

        if customers and orders:
            for cus in customers:
                for order in orders:
                    yield key, (", ".join(cus), ", ".join(order))



            
if __name__ == '__main__':
    Join.run()


        

