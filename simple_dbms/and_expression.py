from conditional_expression import ConditionalExpression


class AndExpression(ConditionalExpression, object):
    """
    A class that represents a conditional expression involving an AND operator.
    An AndExpression involves two conditional subexpressions, and it is true
    when both of the subexpressions are true, and false otherwise.
    """

    def __init__(self, left, right):
        """
        Constructs an AndExpression object that represents the conditional
        expression <code>left AND right</code>.
        :param left: one of the conditionals being anded
        :param right: the other conditional
        """
        super(AndExpression, self).__init__(left, right)

    def is_true(self):
        """
        Evaluates the expression represented by the called object, based on
        the current truth values of the subexpressions.
        :return: true if the two subexpressions are both true, and false otherwise
        """
        return self.get_left().is_true() and self.get_right().is_true()