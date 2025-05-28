Lambda Function
====================

What is the difference between a normal function and a lambda function?

# Normal function example
def mysum(x, y):
    return x + y

total = mysum(5, 7)
print(total)

# Doubling list elements using a normal function
my_list = [5, 3, 3, 2, 3]

def doubler(x):
    return 2 * x

output = list(map(doubler, my_list))
print(output)

# Doubling list elements using a lambda function
output = list(map(lambda x: 2 * x, my_list))
print(output)

# Sum all elements in a list using a normal function
my_list = [5, 3, 3, 2, 3]

def sum_list(x):
    total = 0
    for i in x:
        total += i
    return total

print(sum_list(my_list))

# Sum all elements using reduce and lambda
from functools import reduce
result = reduce(lambda x, y: x + y, my_list)
print(result)

# Note:
# map(), reduce() are higher-order functions because they either
# take a function as an argument or return another function.
