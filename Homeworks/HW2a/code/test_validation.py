
from pandas import read_csv
from datetime import datetime
import numpy as np
import pandas as pd
from pandas.plotting import lag_plot
from statsmodels.graphics.gofplots import ProbPlot
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA

residuals = []
order = (12,0,0)
with open('results/505238_503996/residuals.txt') as f:
	for line in f:
		try:
			resid = float(line)
			residuals.append(resid)
		except:
			print("ERROR")
f.close()
print(residuals)
residuals = pd.DataFrame(residuals)

#Run Sequence
residuals.plot()
pyplot.title(str(order) + ' - Run Sequence')
pyplot.show()
residuals.plot(kind='kde')
pyplot.title(str(order) + ' - Kernel density estimaton')
pyplot.show()
print(residuals.describe())

#Lag Plot
lag_plot(residuals)
pyplot.title(str(order) + ' - Lag Plot')
pyplot.show()

#Histogram
residuals.hist()
pyplot.title(str(order) + ' - Histogram')
pyplot.show()

#Normal Probability
probplot = ProbPlot(residuals)
probplot.qqplot()
pyplot.title(str(order) + ' - Normal Probability Plot')
pyplot.show()