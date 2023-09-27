class DorisColumnItem:
    def __init__(self, col_name, col_type, col_comment, col_default):
        self._col_name = col_name
        self._col_type = col_type
        self._col_comment = col_comment
        self._col_default = col_default

    def get_col_name(self):
        return self._col_name

    def get_col_type(self):
        return self._col_type

    def get_col_comment(self):
        return self._col_comment

    def get_col_default(self):
        return self._col_default

    def get_view_column_constraint(self):
        res = ""
        if self._col_comment != "":
            res  = f"`{self._col_name}` COMMENT '{self._col_comment}'"
        else:
            res = f"`{self._col_name}`"
        return res

    def get_table_column_constraint(self):
        res = ""
        if self._col_type is not None:
            res = f"cast(`{self._col_name}` as {self._col_type}) as `{self._col_name}`"
        else:
            res = f"`{self._col_name}`"
        return res