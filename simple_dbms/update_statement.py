class UpdateStatement:
    def __init__(self, table, update, where):
        self.table = table
        self.update = update
        self.where = where