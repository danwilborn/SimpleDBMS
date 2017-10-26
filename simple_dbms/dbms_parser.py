import ply.yacc as yacc

from column import Column
from select_statement import SelectStatement
from sql_statement import SQLStatement
from table import Table
from or_expression import OrExpression
from and_expression import AndExpression
from not_expression import NotExpression
from comparison import Comparison
from compare_term import CompareTerm
from limit import Limit
from create_statement import CreateStatement
from column_options import ColumnOptions
from insert_statement import InsertStatement
from delete_statement import DeleteStatement
from update_statement import UpdateStatement
from drop_statement import DropStatement
from commit_statement import CommitStatement
from begin_statement import BeginStatement
from rollback_statement import RollbackStatement

# Get the token map from the lexer.  This is required.
from dbms_lexer import tokens


def p_sql_stmt(p):
    r"""sql_stmt : select_stmt SEMICOLON
                | insert_stmt SEMICOLON
                | update_stmt SEMICOLON
                | delete_stmt SEMICOLON
                | create_stmt SEMICOLON
                | drop_stmt SEMICOLON
                | begin_stmt SEMICOLON
                | commit_stmt SEMICOLON
                | rollback_stmt SEMICOLON"""
    p[0] = p[1]


def p_select_stmt(p):
    r"""select_stmt : SELECT distinct_specified select_list FROM from_list where_clause limit"""
    p[0] = SelectStatement(p[3], p[5], p[6], p[7], p[2])


def p_distinct_specified(p):
    r"""distinct_specified :
                            | ALL
                            | DISTINCT"""
    if len(p) == 1:
        p[0] = False
    else:
        if p.slice[1].type == 'DISTINCT':
            p[0] = True
        else:
            p[0] = False


def p_select_list(p):
    r"""select_list : select_item
                    | select_list COMMA select_item"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_select_item(p):
    r"""select_item : column
                    | column_val
                    | STAR"""
    if p.slice[1].type == 'STAR':
        p[0] = SelectStatement.STAR
    else:
        p[0] = p[1]


def p_column(p):
    r"""column : ID
                | table DOT ID"""
    if len(p) == 2:
        p[0] = Column.from_name(p[1])
    elif len(p) == 4:
        p[0] = Column.from_name_table(p[3], p[1])


def p_column_val(p):
    r"""column_val : INTEGER_LITERAL
                  | FLOAT_LITERAL
                  | STRING_LITERAL
                  | NULL"""
    if p[1] == 'NULL':
        p[0] = None
    else:
        p[0] = p[1]


def p_from_list(p):
    r"""from_list : from_item
                    | from_list COMMA from_item"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_from_item(p):
    r"""from_item : table range_variable"""
    p[0] = p[1]
    if p[2] != None:
        p[0].set_range_variable(p[2])


def p_table(p):
    r"""table : ID"""
    p[0] = Table(p[1])


def p_range_variable(p):
    r"""range_variable :
                      | ID
                      | AS ID"""
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[1]


def p_where_clause(p):
    r"""where_clause :
                        | WHERE conditional_expression"""
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[2]


def p_conditional_expression(p):
    r"""conditional_expression : conditional_term
                                | conditional_expression OR conditional_term"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = OrExpression(p[1], p[3])


def p_conditional_term(p):
    r"""conditional_term : conditional_factor
                        | conditional_term AND conditional_factor"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = AndExpression(p[1], p[3])


def p_conditional_factor(p):
    r"""conditional_factor : conditional_primary
                            | NOT conditional_primary"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = NotExpression(p[2])


def p_conditional_primary(p):
    """conditional_primary : comparison
                            | LPAREN conditional_expression RPAREN"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[2]


def p_comparison(p):
    """comparison : cterm EQ cterm
                    | cterm NOTEQ cterm
                    | cterm LT cterm
                    | cterm GT cterm
                    | cterm LTEQ cterm
                    | cterm GTEQ cterm
                    | cterm LIKE STRING_LITERAL
                    | cterm CLIKE STRING_LITERAL
                    | cterm IS NULL
                    | cterm IS NOT NULL"""
    if len(p) == 4:
        if p.slice[2].type == 'EQ':
            p[0] = Comparison(Comparison.EQ, p[1], p[3])
        elif p.slice[2].type == 'NOTEQ':
            p[0] = Comparison(Comparison.NOTEQ, p[1], p[3])
        elif p.slice[2].type == 'LT':
            p[0] = Comparison(Comparison.LT, p[1], p[3])
        elif p.slice[2].type == 'GT':
            p[0] = Comparison(Comparison.GT, p[1], p[3])
        elif p.slice[2].type == 'LTEQ':
            p[0] = Comparison(Comparison.LTEQ, p[1], p[3])
        elif p.slice[2].type == 'GTEQ':
            p[0] = Comparison(Comparison.GTEQ, p[1], p[3])
        elif p.slice[2].type == 'LIKE':
            p[0] = Comparison(Comparison.LIKE, p[1], CompareTerm(p[3]))
        elif p.slice[2].type == 'CLIKE':
            p[0] = Comparison(Comparison.CLIKE, p[1], CompareTerm(p[3]))
        elif p.slice[2].type == 'CLIKE':
            p[0] = Comparison(Comparison.IS_NULL, p[1], None)
    else:
        p[0] = Comparison(Comparison.IS_NOT_NULL, p[1], None)


def p_cterm(p):
    r"""cterm : column
                | INTEGER_LITERAL
                | FLOAT_LITERAL
                | STRING_LITERAL"""
    if p.slice[1].type == 'column':
        p[0] = p[1]
    else:
        p[0] = CompareTerm(p[1])


def p_limit(p):
    r"""limit :
                | LIMIT INTEGER_LITERAL
                | LIMIT INTEGER_LITERAL COMMA INTEGER_LITERAL"""
    if len(p) == 1:
        p[0] = None
    elif len(p) == 3:
        p[0] = Limit(None, p[2])
    else:
        p[0] = Limit(p[2], p[4])


def p_create_stmt(p):
    r"""create_stmt : CREATE TABLE table LPAREN col_def_list RPAREN"""
    p[0] = CreateStatement(p[3], p[5])


def p_col_def_list(p):
    r"""col_def_list : col_def
                    | col_def_list COMMA col_def"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_col_def(p):
    r"""col_def : column INTEGER col_def_opts
                | column CHAR LPAREN INTEGER_LITERAL RPAREN col_def_opts
                | column VARCHAR LPAREN INTEGER_LITERAL RPAREN col_def_opts
                | column REAL col_def_opts"""
    p[0] = p[1]
    if p.slice[2].type.upper() == 'INTEGER':
        p[0].set_type(Column.INTEGER)
        p[0].apply_opts(p[3])
        #p[0].apply_opts(p[6])
    elif p.slice[2].type.upper() == 'CHAR':
        p[0].set_type(Column.CHAR)
        p[0].set_length(p[4])
        p[0].apply_opts(p[6])
    elif p.slice[2].type.upper() == 'VARCHAR':
        p[0].set_type(Column.VARCHAR)
        p[0].set_length(p[4])
        p[0].apply_opts(p[6])
    elif p.slice[2].type.upper() == 'REAL':
        p[0].set_type(Column.REAL)
        p[0].apply_opts(p[3])


def p_col_def_opts(p):
    r"""col_def_opts :
                    | NOT NULL
                    | PRIMARY KEY
                    | NOT NULL PRIMARY KEY
                    | PRIMARY KEY NOT NULL"""
    if len(p) == 1:
        p[0] = None
    if len(p) == 3:
        if p.slice[1].type.upper() == 'NOT' and p.slice[2].type.upper() == 'NULL':
            p[0] = ColumnOptions(True, False)
        if p.slice[1].type.upper() == 'PRIMARY' and p.slice[2].type.upper() == 'KEY':
            p[0] = ColumnOptions(False, True)

    if len(p) == 5:
        p[0] = ColumnOptions(True, True)


def p_insert_stmt(p):
    r"""insert_stmt : INSERT INTO table insert_col_clause VALUES LPAREN insert_val_list RPAREN"""
    p[0] = InsertStatement(p[3], p[4], p[7])


def p_insert_col_clause(p):
    r"""insert_col_clause :
                        | LPAREN insert_col_list RPAREN"""
    if len(p) == 1:
        p[0] = None
    if len(p) == 4:
        p[0] = p[2]


def p_insert_col_list(p):
    r"""insert_col_list : column
                    | insert_col_list COMMA column"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_insert_val_list(p):
    r"""insert_val_list : column_val
                        | insert_val_list COMMA column_val"""
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_delete_stmt(p):
    r"""delete_stmt : DELETE FROM table where_clause"""
    p[0] = DeleteStatement(p[3], p[4])


def p_update_stmt(p):
    r"""update_stmt : UPDATE table SET update_list where_clause"""
    p[0] = UpdateStatement(p[2], p[4], p[5])


def p_update_list(p):
    r"""update_list : column EQ column_val
                    | update_list COMMA column EQ column_val"""
    if len(p) == 4:
        p[0] = [p[1]]
        p[0].set_update_val(p[3])
    else:
        p[3].set_update_val([5])
        p[0] = p[1] + [p[3]]


def p_drop_stmt(p):
    r"""drop_stmt : DROP TABLE table"""
    p[0] = DropStatement(p[3])


def p_begin_stmt(p):
    r"""begin_stmt : BEGIN WORK"""
    p[0] = BeginStatement()


def p_commit_stmt(p):
    r"""commit_stmt : COMMIT optional_work"""
    p[0] = CommitStatement()


def p_rollback_stmt(p):
    r"""rollback_stmt : ROLLBACK optional_work"""
    p[0] = RollbackStatement()


def p_optional_work(p):
    r"""optional_work :
                    | WORK"""


def p_error(p):
    print("Syntax error in input!")


def parser():
    # Build the parser
    return yacc.yacc()

    # s = '''select distinct foo.bar, foo.baz from foo, biz where (foo.bar = 1 and foo.baz = biz.baz) and foo.baz = 'test' limit 1,10;'''
    # result = parser.parse(s)
    # print(result)
