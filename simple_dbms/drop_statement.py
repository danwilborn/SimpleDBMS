from sql_statement import SQLStatement
from catalog import Catalog
from operation_status import OperationStatus
import simple_dbms
try:
    from bsddb import db
except ImportError:
    from bsddb3 import db


class DropStatement(SQLStatement, object):
    def __init__(self, table):
        """
        Constructs a DropStatement object involving the specified table.
        :param table: table to be dropped
        """
        super(DropStatement, self).__init__(tables=[table])

    def execute(self):
        table = self.get_table(0)

        try:
            # Close the table's database and remove the table from
            # the in-memory table cache if necessary.
            table.close()

            # Remove the table's information from the catalog.
            if Catalog.remove_metadata(table) == OperationStatus.NOTFOUND:
                raise Exception(table.get_name() + ": no such table")

            # Remove the underlying database file.

            d = db.DB(simple_dbms.SimpleDBMS.get_env())
            d.remove(filename=table.get_name())

            print "Dropped table " + table.get_name() + "."
        except Exception as e:
            print e
            print "Could not drop table: " + table.name
