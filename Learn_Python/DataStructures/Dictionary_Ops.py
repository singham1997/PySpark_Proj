
"""
Unordered collection of key-value pairs.
Keys are unique.
Mutable.
Keys are immutable types (strings, numbers, tuples).
Operations: get(), keys(), values(), items(), update(), pop().
"""

my_dict = {'a': 1, 'b': 2}
my_dict['c'] = 3
# my_dict.get('b')  # Output: 2

print(my_dict.keys())

for key in my_dict.keys():
    print(key)

for val in my_dict.values():
    print(val)

for key, val in my_dict.items():
    print(f"key: {key}, val: {val}")


print("Check if key exists")

thisdict = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}
if "model" in thisdict:
  print("Yes, 'model' is one of the keys in the thisdict dictionary")

print("Remove items from the key")

thisdict.pop("model")
print(thisdict)

print("Delete the whole dictionary")
# del thisdict
# print(thisdict)

print("To clear the dictionary")
thisdict.clear()