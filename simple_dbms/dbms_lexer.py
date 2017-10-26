import ply.lex as lex

# List of token names.   This is always required
tokens = [
    'ID',
    'STRING_LITERAL',
    'INTEGER_LITERAL',
    'FLOAT_LITERAL',
    'EQ',
    'GT',
    'LT',
    'LTEQ',
    'GTEQ',
    'NOTEQ',
    'LPAREN',
    'RPAREN',
    'COMMA',
    'DOT',
    'STAR',
    'SEMICOLON',
]

# Regular expression rules for simple tokens
t_GT = r'>'
t_LT = r'<'
t_EQ = r'== | ='
t_LTEQ = r'<='
t_GTEQ = r'>='
t_NOTEQ = r'!= | <>'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_COMMA = r','
t_DOT = r'\.'
t_STAR = r'\*'
t_SEMICOLON = r'\;'

reserved = {
    'all': 'ALL',
    'and': 'AND',
    'as': 'AS',
    'begin': 'BEGIN',
    'char': 'CHAR',
    'integer': 'INTEGER',
    'real': 'REAL',
    'clike': 'CLIKE',
    'commit': 'COMMIT',
    'create': 'CREATE',
    'delete': 'DELETE',
    'distinct': 'DISTINCT',
    'drop': 'DROP',
    'from': 'FROM',
    'insert': 'INSERT',
    'int': 'INT',
    'into': 'INTO',
    'is': 'IS',
    'key': 'KEY',
    'like': 'LIKE',
    'limit': 'LIMIT',
    'not | !': 'NOT',
    'null': 'NULL',
    'or': 'OR',
    'primary': 'PRIMARY',
    'rollback': 'ROLLBACK',
    'select': 'SELECT',
    'set': 'SET',
    'table': 'TABLE',
    'update': 'UPDATE',
    'values': 'VALUES',
    'varchar': 'VARCHAR',
    'where': 'WHERE',
    'work': 'WORK',
}

tokens += reserved.values()

def t_FLOAT_LITERAL(t):
    r'[-+]?\d*\.\d+'
    t.value = str(t.value).strip()
    if t.value in reserved:
        t.type = reserved[t.value]
    return t

def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    if t.value in reserved:
        t.type = reserved[t.value]
    return t

# A regular expression rule with some action code
def t_STRING_LITERAL(t):
    r'\"[^\"]*\" | \'[^\']*\''
    t.value = str(t.value).strip().strip("'")
    if t.value in reserved:
        t.type = reserved[t.value]
    return t


def t_INTEGER_LITERAL(t):
    r'(?<![\d.])[0-9]+(?![\d.])'
    t.value = str(t.value).strip()
    if t.value in reserved:
        t.type = reserved[t.value]
    return t



# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)


# A string containing ignored characters (spaces and tabs)
t_ignore = ' \t'


# Error handling rule
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


lexer = lex.lex()
lexer.input("select * from foo where foo.a = 1")
while True:
    tok = lexer.token()
    if not tok:
        break
    print tok
