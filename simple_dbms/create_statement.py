from sql_statement import SQLStatement
from catalog import Catalog
from operation_status import OperationStatus
import simple_dbms
try:
    from bsddb import db
except ImportError:
    from bsddb3 import db


class CreateStatement(SQLStatement, object):
    def __init__(self, table, column_def_list):
        """
        Constructs a CreateStatement object that will create the specified
        table with the specified column information.
        :param table:
        :param column_def_list:
        """
        super(CreateStatement, self).__init__(tables=[table], columns=column_def_list)

    def execute(self):
        table = self.get_table(0)
        try:
            # Add the column information to the Table object, and
            # make sure there is only one primary-key column and no
            # duplicate column names.
            has_pk = False
            for i in range(0, self.num_columns()):
                col = self.get_column(i)
                table.add_column(col)

                if col.is_primary_key():
                    if has_pk:
                        raise (table + ": may not have more than one primary key column")
                    has_pk = True
                for j in range(i + 1, self.num_columns()):
                    if col.get_name() == self.get_column(j).get_name():
                        raise table.get_name() + ": may not have two columns named " + col.get_name()
            # Add the metadata for the table to the catalog, printing
            # an error message if the table name is already in use.
            if Catalog.put_metadata(table) == OperationStatus.KEYEXIST:
                raise AttributeError(table.name + ": a table with this name already exists")

            # create the BDB database for the table
            d = db.DB(simple_dbms.SimpleDBMS.get_env())
            txn = simple_dbms.SimpleDBMS.get_env().txn_begin()
            simple_dbms.SimpleDBMS.push_txn(txn)

            d.set_flags(0)

            if has_pk:
                d.open(filename=table.name, txn=txn, dbtype=db.DB_BTREE,
                                        flags=db.DB_CREATE | db.DB_THREAD, mode=0666)
            else:
                d.open(filename=table.name, txn=txn, dbtype=db.DB_RECNO,
                                        flags=db.DB_CREATE | db.DB_THREAD, mode=0666)
            txn.commit()
            simple_dbms.SimpleDBMS.pop_txn()

            table.set_db(d)

            print "Created table " + table.name + "."
        except Exception as e:
            print e
            print "Could not create table " + table.name + "."

        # Because we could not use Table.open() above, the table is not
        # in the table cache.  Thus, we need to close its DB handle here,
        # or else it will never get closed!
        #
        # In the other commands, we will use Table.open(), so this
        # step will not be necessary.
        if table.get_db() is not None:
            table.get_db().close()
