class CompareTerm:
    INTEGER = 0

    # A real (i.e., floating-point) operand
    REAL = 1

    # A string operand
    STRING = 2

    MINTYPE = 0
    MAXTYPE = 2

    def __init__(self, value=None):
        """
        Default constructor used when constructing a Column object
        """
        if value is None:
            # will set later
            self.val_type = -1
            self.value = None
            return

        self.value = value
        self.val_type = CompareTerm.STRING
        inte = False
        val = value

        try:
            val = int(value)
            self.val_type = CompareTerm.INTEGER
            inte = True
        except:
            pass

        if not inte:
            try:
                val = float(value)
                self.val_type = CompareTerm.REAL
            except:
                pass

        self.value = val

    def set_val_type(self, type):
        """
        Sets the type of the operand's value.
        :param type: the type (one of the constants defined in this class)
        :return:
        """
        if type < CompareTerm.MINTYPE or type > CompareTerm.MAXTYPE:
            raise Exception("invalid value type", type)
        self.val_type = type

    def get_val_type(self):
        """
        Gets the type of the operand's value.
        :return: the operand's type (one of the constants defined in this class)
        """
        return self.val_type

    def get_value(self):
        """
        Gets the operand's value.
        :return: the operand's value
        """
        return self.value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)
