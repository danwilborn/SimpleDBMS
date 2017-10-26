import re
from conditional_expression import ConditionalExpression
from compare_term import CompareTerm


class Comparison(ConditionalExpression, object):
    """
    A class that represents a comparison appearing in a WHERE clause.
    """
    EQ = 0
    NOTEQ = 1
    LT = 2
    GT = 3
    LTEQ = 4
    GTEQ = 5
    LIKE = 6
    CLIKE = 7
    IS_NULL = 8
    IS_NOT_NULL = 9

    MINTYPE = 0
    MAXTYPE = 9

    def __init__(self, type, left, right):
        """
        Constructs a Comparison object that represents a comparison involving
        the specified operator and operands.
        :param type: the type of comparison (i.e., the operator)
        :param left: the left operand
        :param right: the right operand (maybe null)
        """
        super(Comparison, self).__init__(left, right)

        self.type = type
        self.left = left
        self.right = right

        # For LIKE and CLIKE, create a Pattern object for the regex.
        if type == Comparison.LIKE or type == Comparison.CLIKE:
            # Convert SQL regex to regex.
            expr = self.right.get_value()
            expr = expr.replace("%", ".*")
            expr = expr.replace('_', '.')

            if type == Comparison.LIKE:
                self.regex = re.compile(expr)
            else:
                self.regex = re.compile(expr, re.IGNORECASE)

    def get_left_term(self):
        """
        Returns the left operand of the comparison represented by the called object
        :return: the left operand
        """
        return self.left

    def get_right_term(self):
        """
        Returns the right operand of the comparison represented by the called object
        :return: the right operaand (null if it sis a IS_NULL or IS_NOT_NULL)
        """
        return self.right

    def get_type(self):
        """
        Returns the type of the comparison represented by the called object
        :return: the type of the comparison -- i.e., one of the constants
                defined in this class (e.g., Comparison.EQ for equals)
        """
        return self.type

    def is_true(self):
        """
        Evaluates the comparison represented by the called object, based on
        the current values of the operands
        :return: true if the comparison is true, and false if it is false
        """
        left_val = self.left.get_value()
        right_val = None
        if self.right is not None:
            right_val = self.right.get_value()

        # To enable comparisons between integers and reals,
        # we turn all Integers into Doubles.
        if left_val is not None and isinstance(left_val, int):
            left_val = float(left_val)
        if right_val is not None and isinstance(right_val, int):
            right_val = float(right_val)

        # Comparisons with type mismatches are always false.
        if left_val is not None and right_val is not None and \
                        left_val.__class__.__name__ != right_val.__class__.__name__:
            return False

        if self.left.get_val_type() == CompareTerm.INTEGER or self.left.get_val_type() == CompareTerm.REAL:
            return self._compare_args(float(left_val), float(right_val))
        elif self.left.get_val_type() == CompareTerm.STRING:
            if right_val == None:
                return self._compare_args(str(left_val), None)
            else:
                return self._compare_args(str(left_val), str(right_val))
        else:
            raise Exception("Unknown value type", self.left.get_val_type(), "for left side of comparison")

    def _compare_args(self, left_arg, right_arg):
        """
        A helper method that performs the actual comparison.
        :param left_arg:
        :param right_arg:
        :return:
        """
        if self.type is not Comparison.IS_NULL and self.type is not Comparison.IS_NOT_NULL and (
                        left_arg is None or right_arg is None):
            return False

        if self.type == Comparison.EQ:
            return left_arg == right_arg
        elif self.type == Comparison.NOTEQ:
            return left_arg != right_arg
        elif self.type == Comparison.LT:
            return left_arg < right_arg
        elif self.type == Comparison.GT:
            return left_arg > right_arg
        elif self.type == Comparison.LTEQ:
            return left_arg <= right_arg
        elif self.type == Comparison.GTEQ:
            return left_arg >= right_arg
        elif self.type == Comparison.LIKE or self.type == Comparison.CLIKE:
            left_str = str(left_arg)
            if self.regex.match(left_str):
                return True
            else:
                return False
        elif self.type == Comparison.IS_NULL:
            return left_arg is None
        elif self.type == Comparison.IS_NOT_NULL:
            return left_arg is not None
        else:
            raise Exception("Unknown comparison type")
