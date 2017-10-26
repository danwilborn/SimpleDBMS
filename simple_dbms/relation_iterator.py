import abc


class RelationIterator:
    """
    An abstract class for classes that serve as iterators over relations --
    either stored relations (the class TableIterator), or relations formed
    in the process of evaluating a query (the classes CrossIterator and
    ProjectionIterator).
    """

    def __init__(self):
        pass

    @abc.abstractmethod
    def close(self):
        """
        Closes the iterator, which closes any BDB handles that it is using.
        :return:
        """

    @abc.abstractmethod
    def next(self):
        """
        Advances the iterator to the next tuple in the relation.  If
        there is a WHERE clause that limits which tuples should be
        included in the relation, this method will advance the iterator
        to the next tuple that satisfies the WHERE clause.  If the
        iterator is newly created, this method will position it on the first
        tuple in the relation (that satisfies the WHERE clause).
        :return: true if the iterator was advanced to a new tuple, and false
                if there are no more tuples to visit
        """

    @abc.abstractmethod
    def get_column(self, col_index):
        """
        Gets the column at the specified index in the relation that
        this iterator iterates over.  The leftmost column has an index of 0.
        :param col_index:
        :return:
        """

    @abc.abstractmethod
    def get_column_val(self, col_index):
        """
        Gets the value of the column at the specified index in the row
        on which this iterator is currently positioned.  The leftmost
        column has an index of 0.

        This method will unmarshall the relevant bytes from the
        key/data pair and return the corresponding Object -- i.e.,
        an object of type String for CHAR and VARCHAR values, an object
        of type Integer for INTEGER values, or an object of type Double
        for REAL values.
        :param col_index:
        :return: the value of the column
        """

    @abc.abstractmethod
    def get_num_columns(self):
        """
        Gets the number of columns in the relation.
        :return: the number of columns
        """

    @abc.abstractmethod
    def get_num_tuples(self):
        """
        Gets the number of tuples that the iterator has visited.
        :return: the number of tuples visited
        """

    def print_all(self):
        """
        Iterates over all rows in the relation and prints them
        :return:
        """
        # Display column names -- and compute the length of the separator.
        separator_len = 0

        print
        for i in range(0, self.get_num_columns()):
            col = self.get_column(i)
            print " | " + col.get_name(),

            col_width = col.print_width()
            t = ""
            for i in range(len(col.get_name()), col_width):
                t += " "
            print t,

            separator_len += (col_width + 4)

        print " | "
        separator_len += 4

        # Display the separator.
        t = " "
        for i in range(0, separator_len):
            t += "-"
        print t

        # Print the tuples.
        while self.next():
            for i in range(0, self.get_num_columns()):
                val = self.get_column_val(i)
                val_string = "null"
                if val is not None:
                    val_string = str(val)

                print " | " + val_string,

                val_width = len(val_string)
                col_width = self.get_column(i).print_width()
                t = ""
                for j in range(val_width, col_width):
                    t += " "
                print t,
            print " | "
        print
