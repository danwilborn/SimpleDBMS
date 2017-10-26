import unittest
from simple_dbms import SimpleDBMS
from simple_dbms import dbms_parser


class TestSimpleDBMS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SimpleDBMS.init()

    @classmethod
    def tearDownClass(cls):
        sql_stmt = dbms_parser.parser().parse('drop table customer;')
        sql_stmt.execute()
        sql_stmt = dbms_parser.parser().parse('drop table salesperson;')
        sql_stmt.execute()

        SimpleDBMS.shutdown()

    customer_insert = ['''create table customer (
                    cust_id integer primary key,
                    cust_name varchar(25),
                    city varchar(25),
                    country varchar(25),
                    beg_bal integer,
                    cur_bal integer );''',
                       'insert into customer values (100, \'Watabe Bros\', \'Tokyo\', \'Japan\', 45551, 52113);',
                       'insert into customer values (101, \'Maltzl\', \'Salzburg\', \'Austria\', 75314, 77200);',
                       'insert into customer values (105, \'Jefferson\', \'Chicago\', \'USA\', 49333, 57811);',
                       'insert into customer values (110, \'Gomez\', \'Santiago\', \'Chile\', 27400, 35414);']

    customer_select = ['select * from customer;',
                       'select * from customer where cust_id = 100 or cur_bal < 40000;']


    def test_customer(self):
        for cmd in self.customer_insert + self.customer_select:
            try:
                # Parse the command string.
                sql_stmt = dbms_parser.parser().parse(cmd)

                if SimpleDBMS.DEBUG:
                    print sql_stmt

                if sql_stmt is not None:
                    # Execute the SQL command.
                    sql_stmt.execute()
                else:
                    self.fail('Statement did not parse')
            except Exception as e:
                #SimpleDBMS.abort()
                self.fail(str(e))

    salesperson_insert = ['''create table salesperson (
                    salpers_id integer primary key,
                    salpers_name varchar(25),
                    manager_id integer,
                    office varchar(25),
                    comm integer );''',
                          'insert into salesperson values (10, \'Rodney Jones\', 27, \'Chicago\', 10);',
                          'insert into salesperson values (14, \'Masaji Matsu\', 44, \'Tokyo\', 11);',
                          'insert into salesperson values (23, \'Francois Moire\', 35, \'Brussels\', 9);',
                          'insert into salesperson values (37, \'Elena Hermana\', 12, \'B.A.\', 13);',
                          'insert into salesperson values (39, \'Goro Azuma\', 44, \'Tokyo\', 10);',
                          'insert into salesperson values (27, \'Terry Cardon\', \'NULL\', \'Chicago\', 15);',
                          'insert into salesperson values (44, \'Albert Ige\', 27, \'Tokyo\', 12);',
                          'insert into salesperson values (35, \'Brigit Bovary\', 27, \'Brussels\', 11);',
                          'insert into salesperson values (12, \'Buster Sanchez\', 27, \'B.A\', 10);',
                          ]

    salesperson_select = ['select * from salesperson;',
                          'select * from salesperson where salpers_id > 15 and comm <= 13;']

    def test_salesperson(self):
        for cmd in self.salesperson_insert + self.salesperson_select:
            try:
                # Parse the command string.
                sql_stmt = dbms_parser.parser().parse(cmd)

                if SimpleDBMS.DEBUG:
                    print sql_stmt

                if sql_stmt is not None:
                    # Execute the SQL command.
                    sql_stmt.execute()
                else:
                    self.fail('Statement did not parse')
            except Exception as e:
                #SimpleDBMS.abort()
                self.fail(str(e))


if __name__ == '__main__':
    unittest.main()
