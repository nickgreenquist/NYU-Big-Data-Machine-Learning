from pandas import read_csv
from datetime import datetime
import numpy as np
import pandas as pd
from pandas.plotting import lag_plot
from statsmodels.graphics.gofplots import ProbPlot
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA


def parser(x):
	return datetime.fromtimestamp(int(x))

series = read_csv('input/505238_503996.csv', header=None, parse_dates=[0], date_parser=parser, delimiter=',',names=['DateTime','AvgSpeed'])
series = series.set_index('DateTime')

#remove nans and outliers
mean = series.loc[series['AvgSpeed']<=35,'AvgSpeed'].mean()
series.loc[series.AvgSpeed>35,'AvgSpeed']=np.nan
series.fillna(mean, inplace=True)
series.fillna(series.mean(axis=0))

#Fit the model
order = (12,0,0)
model = ARIMA(series, order=order)
model_fit = model.fit()
print(model_fit.summary())

#Run Sequence
residuals = pd.DataFrame(model_fit.resid)
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