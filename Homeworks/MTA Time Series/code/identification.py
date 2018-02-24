from pandas import read_csv
from datetime import datetime
from pandas import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot
from pandas.tools.plotting import autocorrelation_plot
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
	
def parser(x):
	return datetime.fromtimestamp(int(x))

series = read_csv('input/505238_503996.csv', header=None, parse_dates=[0], date_parser=parser, delimiter=',',names=['DateTime','AvgSpeed'])
series = series.set_index('DateTime')

#remove nans and outliers
mean = series.loc[series['AvgSpeed']<=35,'AvgSpeed'].mean()
series.loc[series.AvgSpeed>35,'AvgSpeed']=np.nan
series.fillna(mean, inplace=True)
series.fillna(series.mean(axis=0))

# seasonal difference
#series = series.diff(144)
# trim off the first year of empty data
series = series[:145]

for i in range(2):
      #plot ACF and PACF
    series.plot()
    pyplot.title(str(i) + ' Diff')
    pyplot.show()

    '''autocorrelation_plot(series)
    pyplot.show()'''

    plot_acf(series, lags=140).show()
    pyplot.title(str(i) + ' Diff ACF')
    pyplot.show()

    plot_pacf(series, lags=140).show()
    pyplot.title(str(i) + ' Diff PACF')
    pyplot.show()

    series = series.diff(periods=1)
    series.dropna(inplace=True)
