import random
import string

random.seed(12345)


def generate_random_string(length):
    """Generate a random string of specified length."""
    characters = string.ascii_letters
    return "".join(random.choice(characters) for _ in range(length))


for d in ["1", "2"]:
    command = f"ZADD zset:skiplist:{d}:300"
    for x in range(1, 300 + 1):
        rand_ele = generate_random_string(random.randint(1, 64))
        rand_score = random.random()
        command = command + f" {rand_score} ele:{x}:{rand_ele}"

    print(command)
