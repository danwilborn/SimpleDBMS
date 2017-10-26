from conditional_expression import ConditionalExpression


class TrueExpression(ConditionalExpression):
    """
    A class that represents a conditional expression that is always true.
    It is used when evaluating SQL statements with empty WHERE clauses.
    """

    def is_true(self):
        """
        Evaluates the expression represented by the called object,
        which is always true.
        :return:
        """
        return True
