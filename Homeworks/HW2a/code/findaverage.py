import math
with open("input/500004_500005.csv") as f:
    lines = f.readlines()
    total = 0
    num = 0
    for line in lines:
        speed = float(line.split(",")[1])
        if not math.isnan(speed):
            total += speed
            num += 1
        if speed > 35:
            print(line)
    avg = total / num
    print(avg)
