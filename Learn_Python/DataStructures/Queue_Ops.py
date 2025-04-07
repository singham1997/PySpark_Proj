from collections import deque

queue = deque(["Eric", "John", "Michael"])
queue.append("Terry")
print(queue)                        # Terry arrives. ["Eric", "John", "Michael", "Terry"]
queue.appendleft("Graham")          # Graham arrives.  ["Graham", "Eric", "John", "Michael", "Terry"]
print(queue)

print(queue.pop())          # The last element. It will remove "Terry". ["Graham", "Eric", "John", "Michael"]
print(queue)
print(queue.popleft())          # The first to arrive now leaves. It will remove "Graham". ["Eric", "John", "Michael"]
print(queue)                    # Remaining queue in order of arrival

