import random
import sys

random.seed(12345)


command = sys.argv[1]
count = int(sys.argv[2])

command_str = f"{command} int:{count}"
for x in range(1, count + 1):
    randint = random.randint(1, 1000000)
    command_str = command_str + f" {randint}"

print(command_str)
