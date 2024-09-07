import random
import sys

command_str = "HSET __key__"
count = int(sys.argv[1])
for x in range(1, count + 1):
    command_str = command_str + f" field:{x} __data__"

print(command_str)
