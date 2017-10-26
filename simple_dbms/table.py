try:
    from bsddb import db
except ImportError:
    from bsddb3 import db

from column import Column
from operation_status import OperationStatus
from catalog import Catalog
import simple_dbms as dbms


class Table:
    table_cache = {}
    table_cache_init_size = 25

    def __init__(self, name):
        """
        Constructs a Table object with the specified name.
        :param name:
        """
        self.name = name
        self.db = None
        self.columns = None
        self.range_variable = None

    def open(self):
        """
        Opens the table so that it can be accessed by SQL commands.
        Opens the Database handle for the underlying BDB database, and
        fills in the Table object with the metadata stored in the
        catalog for this table.

        IMPORTANT: this should only be used to open tables that have
        already been created.  New tables should be opened in the code that
        implements the CREATE TABLE command.
        :return: <code>OperationStatus.NOTFOUND</code> if the table does
                not exist, and <code>OperationStatus.SUCCESS</code> otherwise
        """

        # If there is a table with this name in the table cache,
        # use its database handle and copy its column information.
        cached_table = None
        if self.name in Table.table_cache:
            cached_table = Table.table_cache[self.get_name()]
        if cached_table is not None:
            self.db = cached_table.db

            # Create a columns array for this Table object from the
            # columns array of the cached Table object.
            # We create a new array so that the Column objects will
            # have access to information associated with this Table
            # object (e.g., its alias, if any).

            self.columns = []
            for i in range(0, len(cached_table.columns)):
                cached_col = cached_table.get_column(i)
                col = Column(cached_col.get_name(), self)
                col.use_col_info(cached_col)
                self.columns.append(col)

            return OperationStatus.SUCCESS

        # Get the column information from the catalog.
        if self.columns is None:
            ret = Catalog.get_metadata(self)
            if ret == OperationStatus.NOTFOUND:
                raise self.name + ": no such table"

        txn = dbms.SimpleDBMS.get_env().txn_begin()
        dbms.SimpleDBMS.push_txn(txn)

        ret = OperationStatus.SUCCESS

        # Configure the database and open a handle to it.
        try:
            dat_bas = db.DB(dbms.SimpleDBMS.get_env())
            dat_bas.open(filename=self.get_name(), txn=txn, dbtype=db.DB_UNKNOWN, flags=0, mode=0660)
            self.set_db(dat_bas)
        except Exception as e:
            txn.abort()
            dbms.SimpleDBMS.pop_txn()
            print self.name + ": could not open file"
            return OperationStatus.NOTFOUND

        txn.commit()
        dbms.SimpleDBMS.pop_txn()
        Table.table_cache[self.name] = self

        return ret

    def close(self):
        """
        Closes the table and ensures all changes have been written to disk.
        It also removes the table from the in-memory cache of open tables.
        If the <code>Table.cacheClose()</code> method is invoked to close
        all tables, it is <i>not</i> necessary to call this method as well.
        :return:
        """
        # Remove the table from the in-memory cache of open tables.
        t = None
        if Table.table_cache is not None and self.name in Table.table_cache:
            t = Table.table_cache.pop(self.name)

        # Close the handle.  Note that we need to use the Table
        # object that was in the cache, because the handle is stored
        # in that object.
        if t is not None and t.db is not None:
            t.db.close()

    def primary_key_column(self):
        """
        Returns the column (if any) that is the primary key of this table.
        :return: the primary key column, or null if there is no primary key
        """
        if self.columns is None:
            raise AttributeError("this Table object does not " +
                                 "have any columns")
        for i in range(0, len(self.columns)):
            col = self.columns[i]
            if col.is_primary_key():
                return col

        return None

    def db_name(self):
        """
        Returns the name of the table's BDB database file.
        :return: the name of the database file
        """
        return self.name + ".tab.db"

    def get_name(self):
        """
        Returns the name of the table.
        :return: name of the table
        """
        return self.name

    def get_range_variable(self):
        """
        Returns the range variable (if any) associated with this table.
        :return: the range variable
        """
        return self.range_variable

    def get_db(self):
        """
        Returns the database handle for this table's underlying database.
        :return: the db handle
        """
        return self.db

    def set_range_variable(self, range_var):
        self.range_variable = range_var

    def get_column(self, i):
        """
        Returns the column with the specified index in this table.
        The leftmost column has an index of 0.
        :param i: the column id
        :return:
        """
        if self.columns is None:
            raise RuntimeError("This table object does not have any columns")
        return self.columns[i]

    def num_columns(self):
        """
        Returns the number of columns in this table.
        :return: number of columns
        """
        if self.columns is None:
            return 0
        else:
            return len(self.columns)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def set_range_variable(self, r):
        """
        Sets a range variable (i.e., an alias) for this table.  Used
        when there is a SELECT command that uses AS to rename a table
        in the FROM clause.
        :param r: range variable
        :return:
        """
        self.range_variable = r

    def add_column(self, c):
        """
        Adds a column to the list of columns for this table.
        :param c: the column to add
        :return:
        """
        if self.columns is None:
            self.columns = []
        self.columns.append(c)

    def set_db(self, db):
        """
        Sets the database handle for this table's underlying database.
        :param db: the db
        :return:
        """
        self.db = db

    @classmethod
    def cache_init(cls):
        """
        Initializes the in-memory cache of open tables.
        :return:
        """
        cls.table_cache = dict()

    @classmethod
    def cache_close(cls):
        """
        Closes the in-memory cache of open tables, which closes all
        the BDB database handles for the open tables.
        :return:
        """
        if cls.table_cache is not None:
            # Close the databases for all tables in the table cache.
            for t in cls.table_cache.values():

                # Note that we don't call Table.close(), because that
                # removes the table from the hashtable that we are
                # currently interating over, which produces in an
                # exception.

                if t is not None:
                    t.db.close()

        Table.table_cache = None
