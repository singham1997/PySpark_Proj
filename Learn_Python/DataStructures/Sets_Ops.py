# Sets

"""
Unordered collection of unique items.
Mutable.
No duplicates.
Operations: add(), remove(), discard(), union(), intersection(), difference().
"""

my_set = {1, 2, 3}
print(f"Initial elements of set: {my_set}")

my_set.add(4)
print(f"Add element member 4 in the set: {my_set}")  # Output: {1, 2, 3, 4}

my_set.remove(3)
print(f"Remove the element 3 from the set: {my_set}")

if 2 in my_set:
    print(f"Element 2 is present in the set")
