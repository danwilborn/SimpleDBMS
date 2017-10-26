import abc

from comparison import Comparison
from column import Column


class SQLStatement:
    def __init__(self, tables=None, columns=None, column_vals=None, where=None, where_columns=None):
        """
        Default constructor, used in the constructors for subclasses
        that represent statements that don't have any associated state
        (i.e., begin, commit, and rollback).
        """
        self.tables = tables
        self.columns = columns
        self.column_vals = column_vals
        self.where = where
        self.where_columns = where_columns

        if columns is not None and where is not None:
            self.where_columns = []
            SQLStatement.get_columns_from(where, self.where_columns)

    @staticmethod
    def get_columns_from(expr, columns_list):
        """
        helper method that finds all column objects in the specified
        WHERE clause (exp) and adds them to the specified list
        :param expr:
        :param columns_list:
        :return:
        """
        if expr is None:
            return
        elif isinstance(expr, Comparison):
            comp = expr
            left = comp.get_left_term()
            if isinstance(left, Column):
                columns_list.append(left)

            right = comp.get_right_term()
            if isinstance(right, Column):
                columns_list.append(right)
        else:
            SQLStatement.get_columns_from(expr.get_left(), columns_list)
            SQLStatement.get_columns_from(expr.get_right(), columns_list)

    @abc.abstractmethod
    def execute(self):
        """
        Executes this SQL statement.
        """

    def add_table(self, table):
        """
        Adds a table to the list of tables for this statement.
        :param table:
        :return:
        """
        if self.tables is None:
            raise AttributeError("This type of statement does not specify any tables")
        self.tables.append(table)

    def add_column(self, column):
        """
        Adds a column to the list of columns for this statement.
        :param column:
        :return:
        """
        if self.columns is None:
            raise AttributeError("This type of statement does not specify any column")
        self.columns.append(column)

    def add_column_val(self, column_val):
        """
        Adds a column_val value to the list of column values for this statement.
        :param column_val:
        :return:
        """
        if self.column_vals is None:
            raise AttributeError("This type of statement does not specify any column values")
        self.column_vals.append(column_val)

    def get_table(self, i):
        """
        Returns the table with the specified index in this statement.
        The first table has an index of 0.
        :param i:
        :return:
        """
        if self.tables is None:
            raise AttributeError("This type of statement does not specify any tables")
        return self.tables[i]

    def get_column(self, i):
        """
        Returns the column with the specified index in this statement.
        The first column has an index of 0.  This method <i>cannot</i> be
        used to obtain columns from a WHERE clause.  Use
        <code>getWhereColumn()</code> instead.
        :param i:
        :return:
        """
        if self.columns is None:
            raise AttributeError("This type of statement does not specify any column")
        return self.columns[i]

    def get_column_val(self, i):
        """
        Returns the column value with the specified index in this statement.
        The first column value has an index of 0.  This method <i>cannot</i> be
        used to obtain values from an UPDATE statement.  Instead, get the
        column using <code>getColumn()</code> and then use its
        <code>getUpdateVal()</code> method.
        :param i:
        :return:
        """
        if self.column_vals is None:
            raise AttributeError("This type of statement does not specify any column values")
        return self.column_vals[i]

    def get_where_column(self, i):
        """
        Returns the column with the specified index from the WHERE clause
        of this statement.  The first such column has an index of 0.
        :param i:
        :return:
        """
        if self.where_columns is None:
            raise AttributeError("This type of statement does not specify a where clause")
        return self.where_columns[i]

    def get_where(self):
        return self.where

    def num_tables(self):
        """
        Returns the number of tables specified in this statement.
        :return:
        """
        if self.tables is None:
            return 0
        else:
            return len(self.tables)

    def num_columns(self):
        """
        Returns the number of columns specified in this statement.  This
        does <i>not</i> include columns specified in the WHERE clause.
        Use <code>numWhereColumns()</code> to get that value.
        :return:
        """
        if self.columns is None:
            return 0
        else:
            return len(self.columns)

    def num_where_columns(self):
        """
        Returns the number of columns in the WHERE clause of this statement,
        or 0 if there is no WHERE clause.
        :return:
        """
        if self.where_columns is None:
            return 0
        else:
            return len(self.where_columns)

    def num_column_vals(self):
        """
        Returns the number of column values specified in this statement.
        This method <i>cannot</i> be used to obtain the number of values
        in an UPDATE statement.  Instead, use the <code>numColumns()</code>
        method.
        :return:
        """
        if self.column_vals is None:
            return 0
        else:
            return len(self.column_vals)

    def __str__(self):
        ret = str(self.__class__)
        if self.num_tables() > 0 or self.num_columns() > 0 or self.num_column_vals() > 0:
            ret += ':'

        if self.num_tables() > 0:
            ret += '\n    tables:'
            for i in range(0, self.num_tables()):
                ret += '\n      ' + str(i) + ': ' + str(self.tables[i])

        if self.num_columns() > 0:
            ret += '\n    columns:'
            for i in range(0, self.num_columns()):
                ret += '\n      ' + str(i) + ': ' + str(self.columns[i])

        if self.num_column_vals() > 0:
            ret += '\n    column_vals:'
            for i in range(0, self.num_column_vals()):
                ret += '\n      ' + str(i) + ': ' + str(self.column_vals[i])

        if self.num_where_columns() > 0:
            ret += '\n    where_columns:'
            for i in range(0, self.num_where_columns()):
                ret += '\n      ' + str(i) + ': ' + str(self.where_columns[i])

        return ret

    def __repr__(self):
        return self.__str__()
