'''
==============
3D scatterplot
==============

Demonstration of a basic scatterplot in 3D.
'''

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

n = 63

#Map Reduce = 1
ax.scatter(1, 1, 20, c='red')
ax.scatter(1, 10, 21, c='red')
ax.scatter(1, 20, 22, c='red')
ax.scatter(1, 30, 22, c='red')
ax.scatter(1, 50, 23, c='red')
ax.scatter(1, 70, 24.5, c='red')
ax.scatter(1, 100, 26.1, c='red')

#Map Reduce = 8
ax.scatter(8, 1, 19.6, c='blue')
ax.scatter(8, 10, 21.2, c='blue')
ax.scatter(8, 20, 21.1, c='blue')
ax.scatter(8, 30, 22.5, c='blue')
ax.scatter(8, 50, 23.6, c='blue')
ax.scatter(8, 70, 24.2, c='blue')
ax.scatter(8, 100, 27.1, c='blue')

#Map Reduce = 16
ax.scatter(16, 1, 21, c='green')
ax.scatter(16, 10, 20.9, c='green')
ax.scatter(16, 20, 23, c='green')
ax.scatter(16, 30, 22.3, c='green')
ax.scatter(16, 50, 23.3, c='green')
ax.scatter(16, 70, 27.7, c='green')
ax.scatter(16, 100, 26.4, c='green')

#Map Reduce = 24
ax.scatter(24, 1, 21, c='yellow')
ax.scatter(24, 10, 22.3, c='yellow')
ax.scatter(24, 20, 22.1, c='yellow')
ax.scatter(24, 30, 23.1, c='yellow')
ax.scatter(24, 50, 24.2, c='yellow')
ax.scatter(24, 70, 26.2, c='yellow')
ax.scatter(24, 100, 27.0, c='yellow')


#Map Reduce = 39
ax.scatter(39, 1, 21, c='purple')
ax.scatter(39, 10, 23.0, c='purple')
ax.scatter(39, 20, 23.3, c='purple')
ax.scatter(39, 30, 23.6, c='purple')
ax.scatter(39, 50, 26.6, c='purple')
ax.scatter(39, 70, 26.9, c='purple')
ax.scatter(39, 100, 27.4, c='purple')

#Map Reduce = 50
ax.scatter(50, 1, 22, c='orange')
ax.scatter(50, 10, 22.4, c='orange')
ax.scatter(50, 20, 23.2, c='orange')
ax.scatter(50, 30, 25.5, c='orange')
ax.scatter(50, 50, 25.5, c='orange')
ax.scatter(50, 70, 28.7, c='orange')
ax.scatter(50, 100, 27.1, c='orange')

#Map Reduce = 78
ax.scatter(78, 1, 22.9, c='black')
ax.scatter(78, 10, 23.1, c='black')
ax.scatter(78, 20, 23.1, c='black')
ax.scatter(78, 30, 24.0, c='black')
ax.scatter(78, 50, 25.4, c='black')
ax.scatter(78, 70, 27.1, c='black')
ax.scatter(78, 100, 27.3, c='black')

#Map Reduce = 100
ax.scatter(100, 1, 22.2, c='magenta')
ax.scatter(100, 10, 22.1, c='magenta')
ax.scatter(100, 20, 22.2, c='magenta')
ax.scatter(100, 30, 24.0, c='magenta')
ax.scatter(100, 50, 24.4, c='magenta')
ax.scatter(100, 70, 25.2, c='magenta')
ax.scatter(100, 100, 26.2, c='magenta')


ax.set_xlabel('Map Tasks')
ax.set_ylabel('Reduce Tasks')
ax.set_zlabel('Time in seconds')

plt.show()