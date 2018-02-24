from pandas import read_csv
from pandas import datetime
import pandas as pd
from matplotlib import pyplot
from pandas.tools.plotting import autocorrelation_plot
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa import arima_model, ar_model


def parser(x):
	return datetime.strptime('190'+x, '%Y-%m')

series = read_csv('../input/shampoo-sales.csv', header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)

#determine best model to use
'''series_diff = series.diff()[1:]
series_diff.plot()
plot_acf(series_diff).show()
plot_pacf(series_diff).show()
pyplot.show()'''

#from our results, we have determined:
#p = 1, d = 1, q = 1 | 2

#create an ARIMA model using the above orders
model = arima_model.ARIMA(series, order=(4,1,0))
model_fit = model.fit()
print(model_fit.summary())

# plot residual errors
residuals = pd.DataFrame(model_fit.resid)
residuals.plot()
pyplot.show()
residuals.plot(kind='kde')
pyplot.show()
print(residuals.describe())