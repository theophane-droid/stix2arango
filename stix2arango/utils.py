SPECIAL_CHARS = '[()]=<>'
STRING_CHARS = '"\''
SEPARATOR_CHARS = ' \t'


def remove_redondant_parenthesis(expression):
    n = len(expression)
    if (n == 0):
        return
    check = [True] * (n)
    record = []
    result = ""
    i = 0
    while (i < n):
        if (expression[i] == ')'):
            if ((len(record) == 0)):
                return
            elif (expression[record[-1]] == '('):
                check[i] = False
                check[record[-1]] = False
                record.pop()
            else:
                while (not(len(record) == 0) and
                        expression[record[-1]] != '('):
                    record.pop()
                if ((len(record) == 0)):
                    return
                record.pop()
        else:
            record.append(i)
        i += 1
    j = 0
    while (j < n):
        if (check[j]):
            result = result + str(expression[j])
        j += 1
    return result


def check_if_expression_is_balanced(pattern):
    open_tup = tuple('([')
    close_tup = tuple(')]')
    map = dict(zip(open_tup, close_tup))
    queue = []
    is_in_string = False
    string_opener = ''

    for i in pattern:
        if i in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = i
                queue.append(string_opener)
            elif i == string_opener:
                is_in_string = False
                if not queue or string_opener != queue.pop():
                    return False
        elif i in open_tup and not is_in_string:
            queue.append(map[i])
        elif i in close_tup and not is_in_string:
            if not queue or i != queue.pop():
                return False
    if not queue:
        return True
    else:
        return


def remove_unused_space(aql):
    is_in_string = False
    string_opener = ''
    last_char = None
    result = ''
    for c in aql:
        if c in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = c
            elif c == string_opener:
                is_in_string = False
        if c in ')]' and not is_in_string and last_char and last_char == ' ':
            result = result[:-1]
        if c == ' ' and not is_in_string:
            if last_char and (last_char != ' ' and last_char not in '(['):
                result += ' '
        else:
            result += c
        last_char = c
    return result
