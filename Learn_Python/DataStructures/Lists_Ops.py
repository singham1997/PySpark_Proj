# List
"""
Ordered collection of items.
Mutable (can change).
Allows duplicates.
Indexing (starting from 0).
Supports slicing.
Operations: append(), insert(), remove(), pop(), extend(), sort(), reverse().
"""


mylist = [1,2,3]
mylist.append(4)
print(mylist)

mylist.insert(1, 5)
print(mylist)

mylist.remove(5)
print(mylist)

mylist.pop()
print(mylist)

mylist.extend([4,5,6])
print(mylist)

mylist.sort()
print(mylist)

mylist.reverse()
print(mylist)

print(mylist.count(2))

mylist.clear()
print(mylist)


# We can use list as a stack also

# For using deque we need bring it from the collections

# List Comprehensions

print("Without List Comprehension")
squares = []
for x in range(10):
    squares.append(x**2)

print(squares)

print("With List Comprehension")

squares = [x**2 for x in range(10)]
print(squares)

print("List compreshension for 2D vector")

vec = [[1,2,3], [4,5,6], [7,8,9]]
vec = [num for elem in vec for num in elem]
print(vec)

print("Nested List comprehension")

matrix = [
    [1, 2, 3, 4],
    [5, 6, 7, 8],
    [9, 10, 11, 12],
]

res = [[row[i] for row in matrix] for i in range(4)]
print(res)

print("Take single vector input with split of space")

in1 = [int(inp) for inp in input()]
print(in1)

print("Take multi vector input with split of space")

in2 = [[int(inp) for inp in input().split()] for _ in range(int(input()))]

print(in2)