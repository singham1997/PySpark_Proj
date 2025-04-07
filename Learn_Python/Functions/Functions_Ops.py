def my_function():
  print("Hello from a function")

print("Functions with arguments")

def my_function(fname, lname):
  print(fname + " " + lname)

my_function("Emil", "Refsnes")

print("Functions with arbitary args")

def my_function(*kids):
  print("The youngest child is " + kids[2])

my_function("Emil", "Tobias", "Linus")

print("Functions with kwargs")

def my_function(**kid):
  print("His last name is " + kid["lname"])

my_function(fname = "Tobias", lname = "Refsnes")

print("Recursion problem")

def tri_recursion(k):
  if(k > 0):
    result = k + tri_recursion(k - 1)
    print(result)
  else:
    result = 0
  return result

print("Recursion Example Results:")
tri_recursion(6)

print("Lambda Functions")

x = lambda a, b : a * b
print(x(5, 6))

print("Using lambda functions for sorting the dictionary using second item of tuple")

# List of tuples
items = [(1, 'banana'), (2, 'apple'), (3, 'cherry')]

# Sort by second element (fruit name)
sorted_items = sorted(items, key=lambda x: x[1])
print(sorted_items)

print("Reduce functions using lambda")

from functools import reduce

# List of numbers
numbers = [1, 2, 3, 4]

# Reduce to calculate the sum
sum_result = reduce(lambda x, y: x + y, numbers)
print(sum_result)
