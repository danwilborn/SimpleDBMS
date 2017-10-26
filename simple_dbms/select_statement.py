from sql_statement import SQLStatement
from column import Column
from operation_status import OperationStatus
from table_iterator import TableIterator


class SelectStatement(SQLStatement, object):
    STAR = '*'

    def __init__(self, select_list, from_list, where, limit, distinct_specified):
        """
        Constructs a SelectStatement object involving the specified
        columns and other objects from the SELECT clause, the specified
        tables from the FROM clause, the specified conditional
        expression from the WHERE clause (if any), the specified Limit
        object summarizing the LIMIT clause (if any), and the specified
        value indicating whether or not we should eliminate duplicates.
        :param select_list: the columns and other objects from the SELECT clause
        :param from_list: the list of tables from the FROM clause
        :param where: the conditional expression from the WHERE clause (if any)
        :param limit: summarizes the info in the LIMIT clause (if any)
        :param distinct_specified: should duplicates be eliminated?
        """
        super(SelectStatement, self).__init__(tables=from_list, columns=[], where= where)
        self.select_list = select_list
        self.from_list = from_list
        self.where = where
        self.limit = limit
        self.distinct_specified = distinct_specified

        # add the columns in the select list to the list of columns
        for select_item in select_list:
            if isinstance(select_item, Column):
                self.add_column(select_item)

    def is_distinct_specified(self):
        """
        Returns a boolean value indicating whether duplicates should be
        eliminated in the result of this statement -- i.e., whether the
        user specified SELECT DISTINCT.
        :return:
        """
        return self.distinct_specified

    def execute(self):
        """

        :return:
        """
        _iter = None

        try:
            if self.num_tables() == 1:
                table = self.get_table(0)
                if table.open() == OperationStatus.NOTFOUND:
                    raise Exception()  # error msg in open()

                if self.num_columns() == 0 and self.select_list[0] == SelectStatement.STAR:
                    _iter = TableIterator(self, table, True)
                else:
                    raise Exception("Can't handle projections yet")
            else:
                raise Exception("Can't yet handle multiple tables in from clause")

            # Iterate over all tuples in the relation and print them out.
            _iter.print_all()

            # Report the number of tuples that were selected.
            num_rows = _iter.get_num_tuples()
            print  "Selected " + str(num_rows),
            if num_rows == 1:
                print "tuple."
            else:
                print "tuples."

        except Exception as e:
            print e

        if _iter is not None:
            _iter.close()
