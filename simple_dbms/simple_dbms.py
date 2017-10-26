import errno
import os
try:
    from bsddb import db
except ImportError:
    from bsddb3 import db

import dbms_parser
import catalog
import table


class SimpleDBMS:
    """
    The main class of a simple relational database.  It includes the
    main() method, and methods for performing tasks or obtaining state
    involving the DBMS application as a whole (e.g., initialization,
    configuring the environment, aborting the application, etc.)
    To allow access to the DBMS methods from all other classes, we make
    all methods static, so that the class name can be used to invoke them.
    To run the application, enter the command<br>
    <br>
    <code>java DBMS</code><br>
    <br>
    from the command line.
    """

    # Set this to true to print debugging messages, and false to
    # omit them.
    DEBUG = True

    # The home directory of the BDB environment.
    DB_HOME = "./"

    env = None

    # used to keep track of the current transaction
    txn_stack = []

    @classmethod
    def main(cls):
        """
        The main method for the DBMS application.
        """

        cls.init()

        while True:
            try:
                # Get the command string from the user.
                print
                command_string = raw_input('Enter command (q to quit): ')
                command_string = command_string.lower()

                if command_string == 'q':
                    break

                # Parse the command string.
                sql_stmt = dbms_parser.parser().parse(command_string)

                if cls.DEBUG:
                    print sql_stmt

                if sql_stmt is not None:
                    # Execute the SQL command.
                    sql_stmt.execute()
            except Exception as e:
                print "Error: " + str(e)
                cls.abort()

        cls.shutdown()

    @classmethod
    def init(cls):
        """
        Initializes the DBMS -- initializing the underlying BDB environment,
        the catalog, and the in-memory cache of open tables.
        :return:
        """
        try:
            cls._environment_init()
            catalog.Catalog.open()
            table.Table.cache_init()
            txn_stack = []
        except RuntimeError as e:
            print e
            cls.abort()

    @classmethod
    def shutdown(cls):
        # Prepares the DBMS to shutdown -- closing all open tables, as well
        # as the catalog and the underlying BDB environment.
        try:
            while len(cls.txn_stack) > 0:
                txn = cls.txn_stack.pop()
                txn.abort()

            table.Table.cache_close()
            catalog.Catalog.close()
            if cls.env is not None:
                cls.env.close()
        except RuntimeError as e:
            print e
            exit(1)

    @classmethod
    def abort(cls):
        cls.shutdown()
        exit(1)

    @classmethod
    def get_env(cls):
        """
        Returns the handle for the underlying BDB environment.
        :return: the environment handle
        """
        return cls.env

    @classmethod
    def get_txn(cls):
        """
        Returns the transaction handle at the top of the transaction
        stack, without removing it from the stack.

        :return: the transaction handle, or null if the transaction stack
            is empty
        """
        if len(cls.txn_stack) == 0:
            return None
        else:
            return cls.txn_stack[-1]

    @classmethod
    def push_txn(cls, txn):
        """
        Stores the specified transaction handle at the top of the
        transaction stack.
        :param txn: the transaction handle to be stored
        :return:
        """
        cls.txn_stack.append(txn)

    @classmethod
    def pop_txn(cls):
        """
        Removes the transaction handle at the top of the transaction
        stack and returns it.
        :return: the transaction handle, or null if the transaction stack
            is empty
        """
        if len(cls.txn_stack) == 0:
            return None
        else:
            cls.txn_stack.pop()

    @classmethod
    def _environment_init(cls):
        """
        Configures and opens the handle for the underlying DB environment
        :return:
        """
        # Create the DB home directory, if necessary.
        home = cls.DB_HOME
        if not os.path.exists(os.path.dirname(home)):
            try:
                os.makedirs(os.path.dirname(home))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        e = db.DBEnv()
        e.set_tx_max(1000)
        e.set_lk_max_lockers(2000)
        e.set_lk_max_locks(2000)
        e.set_lk_max_objects(2000)
        e.set_flags(db.DB_TXN_NOSYNC, True)
        e.open(home, db.DB_INIT_TXN | db.DB_PRIVATE | db.DB_CREATE | db.DB_THREAD | db.DB_INIT_LOCK | db.DB_INIT_MPOOL,
               0660)

        cls.env = e
