from sql_statement import SQLStatement
from operation_status import OperationStatus
from insert_row import InsertRow
try:
    from bsddb import db
except ImportError:
    from bsddb3 import db
import simple_dbms as dbms


class InsertStatement(SQLStatement, object):
    def __init__(self, table, col, val):
        """
        Constructs an InsertStatement object involving the specified table,
        list of columns (if any), and list of values.  The columns and
        their associated values must be specified in the same order.
        If no list of columns is specified, we will assume that values are
        being specified for all columns.
        :param table: table in which the values should be inserted
        :param col: the list of columns for which values are specified
        :param val: the lit of values to be inserted
        """
        super(InsertStatement, self).__init__(tables=[table], columns=col, column_vals=val)

    def execute(self):
        """
        :return:
        """
        try:
            table = self.get_table(0)
            if table.open() != OperationStatus.SUCCESS:
                raise Exception()  # error msg was printed in open()

            # Preliminary error checking.
            if self.num_columns() != 0:
                raise Exception("INSERT commands with column names are not supported")
            if self.num_column_vals() != table.num_columns():
                raise Exception("Must specify a value for each column")

            # Make any necessary adjustments (type conversions,
            # truncations, etc.) to the values to be inserted.
            # This will throw an exception if a value is invalid.
            adjusted_values = []
            for i in range(0, table.num_columns()):
                col = table.get_column(i)
                adjusted_values.append(col.adjust_value(self.get_column_val(i)))

            # Create an InsertRow object for the row to be inserted,
            # and use that object to marshall the row.
            row = InsertRow(table, adjusted_values)
            row.marshall() #TODO HW4 This function needs to be implemented.

            # Perform the insertion.
            ret = OperationStatus.SUCCESS
            key = str(row.get_key().stream.getvalue())
            data = str(row.get_data().stream.getvalue())

            txn = dbms.SimpleDBMS.get_env().txn_begin()
            dbms.SimpleDBMS.push_txn(txn)

            # data processor
            if table.primary_key_column() is not None:
                try:
                    table.get_db().put(key, data, txn=txn, flags=db.DB_NOOVERWRITE)
                except db.DBError:
                    raise Exception("There is an existing row with the specificed primary key")
            else:
                key = ""
                ret = table.get_db().append(data, txn=txn)

            txn.commit()
            dbms.SimpleDBMS.pop_txn()
            print "Added one row to " + table.get_name() + "."

        except Exception as e:
            print "Count not insert row: " + str(e)
            return OperationStatus.ERROR

        return ret
