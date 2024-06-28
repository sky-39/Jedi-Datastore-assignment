import csv
import ast
import operator

# Function to parse the filter expression
def parse_filter_expression(expression):
    try:
        return ast.parse(expression, mode='eval').body
    except SyntaxError as e:
        raise ValueError(f"Invalid filter expression: {expression}") from e

# Function to evaluate the parsed filter expression
def eval_expression(expr, row):
    ops = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.And: lambda *args: all(args),
        ast.Or: lambda *args: any(args)
    }
    
    if isinstance(expr, ast.BoolOp):
        op = ops[type(expr.op)]
        return op(*(eval_expression(value, row) for value in expr.values))
    elif isinstance(expr, ast.Compare):
        left = eval_expression(expr.left, row)
        comparisons = (ops[type(op)](left, eval_expression(right, row)) for op, right in zip(expr.ops, expr.comparators))
        return all(comparisons)
    elif isinstance(expr, ast.Name):
        return convert_value(row[expr.id])
    elif isinstance(expr, ast.Constant):
        return expr.value
    elif isinstance(expr, ast.Call) and expr.func.id == 'len':
        # Handle len function
        arg_value = eval_expression(expr.args[0], row)
        return len(arg_value)
    else:
        raise ValueError(f"Unsupported expression type: {type(expr)}")

# Function to convert CSV values to appropriate types for comparison
def convert_value(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

# Function to filter rows based on the filter expression
def filter_rows(reader, expression_tree):
    for row in reader:
        if eval_expression(expression_tree, row):
            yield row

# Main function to read CSV and apply filter with pagination
def read_csv_with_filter(file_path, filter_criteria, page_number=1, page_size=50):
    with open(file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        expression_tree = parse_filter_expression(filter_criteria)
        
        filtered_rows = filter_rows(reader, expression_tree)
        start_index = (page_number - 1) * page_size
        
        # Skip to the start of the desired page
        for _ in range(start_index):
            next(filtered_rows, None)
        
        # Collect the rows for the current page
        page_rows = []
        for _ in range(page_size):
            row = next(filtered_rows, None)
            if row is None:
                break
            page_rows.append(row)
        
        return page_rows

# Example usage:
file_path = 'data.csv'
filter_criteria = '((major == "Engineering") and (age != 45)) or (tuition_fee <= 16000)'
page_number = 1
page_size = 50

rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
for row in rows:
    print(row)




# # Additional filter criteria examples

# # Filter by Department and Age Range
# filter_criteria = '(department == "Marketing") and (age >= 30) and (age <= 40)'
# rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
# print("Filter by Department and Age Range:")
# for row in rows:
#     print(row)
# print()

# # Filter by Salary and Exclude a Specific Department
# filter_criteria = '(salary > 70000) and (department != "HR")'
# rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
# print("Filter by Salary and Exclude a Specific Department:")
# for row in rows:
#     print(row)
# print()

# # Filter by Multiple Departments and Age
# filter_criteria = '((department == "Engineering") or (department == "Sales")) and (age < 35)'
# rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
# print("Filter by Multiple Departments and Age:")
# for row in rows:
#     print(row)
# print()

# # Filter by Name Length and Salary Range
# filter_criteria = '(len(name) > 8) and (salary >= 50000) and (salary <= 100000)'
# rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
# print("Filter by Name Length and Salary Range:")
# for row in rows:
#     print(row)
# print()

# # Filter by Exact Name and Department
# filter_criteria = '(name == "John Doe") and (department == "Engineering")'
# rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
# print("Filter by Exact Name and Department:")
# for row in rows:
#     print(row)
# print()

# Complex Nested Conditions
filter_criteria = '((major == "Engineering") and (age != 45)) or (tuition_fee <= 60000)'
rows = read_csv_with_filter(file_path, filter_criteria, page_number, page_size)
print("Complex Nested Conditions:")
for row in rows:
    print(row)
print()