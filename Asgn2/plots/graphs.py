from matplotlib import pyplot as plt
import numpy as np

d = [None]*5

d[0] = {0: 0, 1: 4566, 2: 5434}
d[1] = {0: 0, 1: 2522, 2: 4354, 3: 3124}
d[2] = {0: 0, 1: 2164, 2: 3662, 3: 2212, 4: 1962}
d[3] = {0: 0, 1: 1461, 2: 2645, 3: 1963, 4: 1705, 5: 2226}
d[4] = {0: 0, 1: 1177, 2: 1823, 3: 1259, 4: 1678, 5: 1927, 6: 2136} 

x = [i for i in range(2, 7)]
y = [np.std(list(d[i].values())[1:]) for i in range(5)]

plt.plot(x, y)
plt.savefig('SD-1.jpg')