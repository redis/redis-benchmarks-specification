import random
random.seed(12345)

command = "SADD intset:100"
for x in range (1,101):
    randint = random.randint(1,1000000)
    command = command + f" {randint}"

print(command)