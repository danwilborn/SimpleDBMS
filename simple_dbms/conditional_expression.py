import abc


@abc.abstractmethod
class ConditionalExpression:
    """
    An abstract class that represents a conditional expression like those
    found in the WHERE clause of a SQL statement.
    """

    def __init__(self, left=None, right=None):
        """
         Constructor used when constructing an object of a subclass that
        has one or two subexpressions.
        :param left: one of the conditional subexpressions
        :param right: the other conditional subexpression
        """
        self.left = left
        self.right = right

    def get_left(self):
        """
        Returns the left (i.e., the first) subexpression of the
        expression represented by the called object.
        :return: the left subexpression
        """
        return self.left

    def get_right(self):
        """
        Returns the right (i.e., the second) subexpression of the
        expression represented by the called object.
        :return: the right subexpression
        """
        return self.right

    @abc.abstractmethod
    def is_true(self):
        """
        Evaluates the expression represented by the called object, based on
        the current truth values of the subexpressions.
        :return: true if the expression is true, and false if it is false!
        """
