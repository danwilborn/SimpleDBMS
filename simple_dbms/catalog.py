try:
    from bsddb import db
except ImportError:
    from bsddb3 import db

from data_output_stream import DataOutputStream
from data_input_stream import DataInputStream
from operation_status import OperationStatus
from column import Column
from column_options import ColumnOptions
import simple_dbms as dbms


class Catalog:
    """
    A class representing the catalog of a simple relational database.  It
    stores all per-table metadata: the name of the table, and the metadata
    for each of the table's columns.

    To allow access to the catalog methods from all other classes, we make
    all methods static, so that the class name can be used to invoke them.
    """
    catalog_name = 'catalog.db'
    catalog_db = None

    # env = None

    @classmethod
    def open(cls):
        """
        Opens the catalog so that it can be used to store and retrieve metadata.
        :return:
        """
        cls.catalog_db = db.DB(dbms.SimpleDBMS.get_env())
        txn = dbms.SimpleDBMS.get_env().txn_begin()
        dbms.SimpleDBMS.push_txn(txn)
        cls.catalog_db.open(filename=Catalog.catalog_name, txn=txn, dbtype=db.DB_BTREE,
                            flags=db.DB_CREATE | db.DB_THREAD, mode=0666)
        txn.commit()
        dbms.SimpleDBMS.pop_txn()
        # Catalog.catalog_db = bsddb.btopen(Catalog.catalog_name)

    @staticmethod
    def close():
        """
        Closes the catalog and ensures all changes have been written to disk.
        :return:
        """
        if Catalog.catalog_db is not None:
            Catalog.catalog_db.close()

    @staticmethod
    def put_metadata(table):
        """
        Adds an entry to the catalog for the specified table.
        :param table: the table whose metadata we need to add
        :return: <code>OperationStatus.KEYEXIST</code> if there is already
            a table with the same name, and
            <code>OperationStatus.SUCCESS</code> otherwise.
        """
        if table.num_columns() <= 0:
            raise ('cannot add a table to the catalog until '
                   'its column info has been initialized')

        # prepare output
        tuple_output = DataOutputStream()

        # Start with the number of columns
        tuple_output.write_byte(table.num_columns())

        # Then write the metadata for each of the table's columns.

        for i in range(0, table.num_columns()):
            col = table.get_column(i)

            # Write the length of the column name, followed by the name
            col_name = col.get_name()
            tuple_output.write_byte(len(col_name))
            b = bytearray()
            b.extend(col_name)
            for c in b:
                tuple_output.write_byte(c)

            # Write the rest of the column information.
            tuple_output.write_byte(col.get_type())
            tuple_output.write_int(col.get_length())
            tuple_output.write_boolean(col.is_not_null())
            tuple_output.write_boolean(col.is_primary_key())

        # Prepare the key and data item
        key = table.get_name()
        data = str(tuple_output.stream.getvalue())
        txn = dbms.SimpleDBMS.get_env().txn_begin()
        dbms.SimpleDBMS.push_txn(txn)
        try:
            Catalog.catalog_db.put(key, data, txn=txn, flags=db.DB_NOOVERWRITE)
        except db.DBError:
            return OperationStatus.KEYEXIST

        txn.commit()
        Catalog.catalog_db.sync()
        dbms.SimpleDBMS.pop_txn()

        return OperationStatus.SUCCESS

    @staticmethod
    def get_metadata(table):
        """
        Reads the entry for the specified table from the catalog of
        metadata and adds the column information to the Table object.
        :param table: the table whose metadata we need to retrieve
        :return: <code>OperationStatus.NOTFOUND</code> if the table is not in
            the catalog, and <code>OperationStatus.SUCCESS</code> otherwise.
        """
        # Retrieve the catalog entry for the table.
        key = table.get_name()
        txn = dbms.SimpleDBMS.get_env().txn_begin()
        dbms.SimpleDBMS.push_txn(txn)

        try:
            data = Catalog.catalog_db.get(key, txn=txn)
        except db.DBError as e:
            txn.abort()
            dbms.SimpleDBMS.pop_txn()
            return OperationStatus.NOTFOUND

        txn.commit()
        dbms.SimpleDBMS.pop_txn()

        # Process the table info. from the catalog entry.

        # We use a inputstream object, and read the individual pieces
        # of info. in the same order that they were written in
        # put_metadata().
        tuple_input = DataInputStream(data)

        # First, the number of columns.
        num_columns = tuple_input.read_byte()

        # Next, the metadata for each column.
        # We create the corresponding Column objects and add them to the
        # Table object.
        for i in range(0, num_columns):
            # the column name
            col_name_length = tuple_input.read_byte()
            col_name = bytearray()
            for j in range(0, col_name_length):
                col_name.append(tuple_input.read_byte())
            col = Column.from_name_table(str(col_name), table)

            # the column info
            col.set_type(tuple_input.read_byte())
            col.set_length(tuple_input.read_int())
            opts = ColumnOptions(tuple_input.read_boolean(), tuple_input.read_boolean())
            col.apply_opts(opts)
            col.set_index(i)

            # add the column to the table
            table.add_column(col)

        return OperationStatus.SUCCESS

    @staticmethod
    def remove_metadata(table):
        """
        Removes the entry for the specified table from the catalog of metadata.
        :param table: the table whose metadata we need to remove
        :return: <code>OperationStatus.NOTFOUND</code> if the table is not in
            the catalog, and <code>OperationStatus.SUCCESS</code> otherwise.
        """
        key = table.get_name()
        txn = dbms.SimpleDBMS.get_env().txn_begin()
        dbms.SimpleDBMS.push_txn(txn)
        try:
            ret = Catalog.catalog_db.delete(key, txn=txn)
        except db.DBError as e:
            txn.abort()
            dbms.SimpleDBMS.pop_txn()
            return OperationStatus.NOTFOUND

        txn.commit()
        dbms.SimpleDBMS.pop_txn()

        return OperationStatus.SUCCESS
