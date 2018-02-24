from pandas import read_csv
from datetime import datetime
from pandas import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot
import itertools
from pandas.tools.plotting import autocorrelation_plot
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error

# evaluate an ARIMA model for a given order (p,d,q)
def evaluate_arima_model(df, arima_order):
	X = df.values
	#size = int(len(X) * .95)
	#train, test = X[0:size], X[size:len(X)]
	size = len(X) - 144
	train, test = X[0:size], X[size:len(X)]
	history = [x for x in train]
	predictions = list()
	for t in range(len(test)):
		model = ARIMA(history, order=arima_order)
		#model = SARIMAX(history, order=arima_order, seasonal_order=(1,1,1,144))
		model_fit = model.fit(disp=0)
		output = model_fit.forecast()
		yhat = output[0]
		predictions.append(yhat)
		obs = test[t]
		history.append(obs)
		print('predicted=%f, expected=%f' % (yhat, obs))
		residual = yhat - obs
		with open("results.txt", 'a') as f:
			f.write('\n'+str(residual[0]))
		f.close()
	error = mean_squared_error(test, predictions)
	print('Test MSE: %.3f' % error)
	# plot
	pyplot.plot(test)
	pyplot.plot(predictions, color='red')
	pyplot.title(str(arima_order))
	pyplot.show()

	residuals = pd.DataFrame(predictions.resid)
	residuals.plot()
	pyplot.show()

	return error

# evaluate combinations of p, d and q values for an ARIMA model
def evaluate_models(dataset, p_values, d_values, q_values):
	best_score, best_cfg = float("inf"), None
	for p in p_values:
		for d in d_values:
			for q in q_values:
				order = (p,d,q)
				try:
					mse = evaluate_arima_model(dataset, order)
					if mse < best_score:
						best_score, best_cfg = mse, order
					print('ARIMA%s MSE=%.3f' % (order,mse))
				except:
					continue
	print('Best ARIMA%s MSE=%.3f' % (best_cfg, best_score))

def fit_models(dataset, p_values, d_values, q_values):
	best_score, best_cfg = float("inf"), None
	for p in p_values:
		for d in d_values:
			for q in q_values:
				order = (p,d,q)
				try:
					print("\n\n\n\n")
					model = ARIMA(dataset, order=order)
					model_fit = model.fit()
					print(model_fit.summary())

					if model_fit.aic < best_score:
						best_score, best_cfg = model_fit.aic, order
					print('ARIMA{} - AIC:{}'.format(order, model_fit.aic))
					with open("results.txt", 'a') as f:
						f.write('\nARIMA{} - AIC:{}'.format(order, model_fit.aic))
					f.close()

					# plot residual errors
					residuals = pd.DataFrame(model_fit.resid)
					residuals.plot()
					pyplot.show()
					residuals.plot(kind='kde')
					pyplot.show()
					print(residuals.describe())
				except:
					continue
	print('Best ARIMA%s AIC=%.3f' % (best_cfg, best_score))
	with open("results.txt", 'a') as f:
		f.write('\nBest ARIMA%s AIC=%.3f' % (best_cfg, best_score))
	f.close()

def parser(x):
	return datetime.fromtimestamp(int(x))

series = read_csv('input/505238_503996.csv', header=None, parse_dates=[0], date_parser=parser, delimiter=',',names=['DateTime','AvgSpeed'])
series = series.set_index('DateTime')

#remove nans and outliers
mean = series.loc[series['AvgSpeed']<=35,'AvgSpeed'].mean()
series.loc[series.AvgSpeed>35,'AvgSpeed']=np.nan
series.fillna(mean, inplace=True)
series.fillna(series.mean(axis=0))

#smaller sample
#series = series[:3024]

# seasonal difference
#series = series.diff(144)
# trim off the first year of empty data
#series = series[144:]


# GRID SEARCH
p_values = [0,1,2,4,6,8,10,12]
d_values = [0,1]
q_values = [0,1,2]

#evaluate_models(series, p_values, d_values, q_values)
#evaluate_models(series, [0], [1], [0])

#create an ARIMA model with one order
evaluate_arima_model(series, (10, 0, 2))

#grid search for best AIC
#fit_models(series, p_values, d_values, q_values)