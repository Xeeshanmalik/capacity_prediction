import numpy
import pandas
import tkinter
import matplotlib.pyplot as plt
import math
import os
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
import pandas as pd
from keras import optimizers
import boto3
import pickle
from collections import defaultdict

algo="RNN"
mode = "output"
cbu="SW0"
phub="PL01"
cmts="cmts77.lndn"
macdomain="CATV-MAC 5"
number_of_predictions=100

path = os.path.join("s3://video-poc-test-case", algo,"output",cbu, phub, cmts, macdomain, "_actual","part-00000")
dataframe = pd.read_csv(path,sep = ",", header=0, names=["date","capacity"],engine='python', skipfooter=3)
dataframe = dataframe.sort_values(by=['date'],ascending=True)
dataframe = dataframe.tail(120)
dates = dataframe.loc[:, 'date']
last_date = dates.tail(1).values
dataset = dataframe.loc[:, 'capacity']
dataset = dataset.values
dataset = numpy.expand_dims(dataset,axis=1)
dataset = dataset.astype('long')

scaler = MinMaxScaler(feature_range=(0, 1))
dataset = scaler.fit_transform(dataset)
sgd = optimizers.SGD(lr=0.1)

train_size = int(len(dataset) * 0.67)
back_test_size = len(dataset) - train_size
test_size = number_of_predictions
dataset_with_test_predictions = numpy.random.rand(dataset.shape[0]+test_size,1)

train, back_test, test  = dataset[0:train_size,:], dataset[train_size:len(dataset),:], dataset[0:test_size,:]


#  convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return numpy.array(dataX), numpy.array(dataY)

def generate_dates(number_of_predictions, last_date, count):
    output = []
    string_dte = last_date[0]
    string_split = string_dte.split("/")
    month = int(string_split[0])
    day = int(string_split[1])
    year= int(string_split[2])
    for j in range(0,count):
        if day < 31:
            day += 1
        else:
            day = 1
            month += 1
        if month >= 12:
            month=1
        output.append(str(month) + "/" + str(day) + "/" + str(year))


    return output

# reshape into X=t and Y=t+1
look_back = 1
trainX, trainY = create_dataset(train, look_back)
back_testX, back_testY = create_dataset(back_test, look_back)
testX, testY = create_dataset(test, look_back)

# reshape input to be [samples, time steps, features]
trainX = numpy.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
back_testX = numpy.reshape(back_testX, (back_testX.shape[0], 1, back_testX.shape[1]))
testX = numpy.reshape(testX, (testX.shape[0], 1, testX.shape[1]))


# create and fit the LSTM network
model = Sequential()
model.add(LSTM(5, input_dim=look_back))
# model.add(Dropout(0.1))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(trainX, trainY, nb_epoch=20, batch_size=10, verbose=1)

# make predictions
trainPredict = model.predict(trainX)
bktestPredict = model.predict(back_testX)
testPredict = model.predict(testX)

# invert predictions
trainPredict = scaler.inverse_transform(trainPredict)
trainY = scaler.inverse_transform([trainY])
bktestPredict = scaler.inverse_transform(bktestPredict)
back_testY = scaler.inverse_transform([back_testY])
testPredict = scaler.inverse_transform(testPredict)
testY = scaler.inverse_transform([testY])
trainPredictPlot = numpy.empty_like(dataset)
trainPredictPlot[:, :] = numpy.nan
trainPredictPlot[look_back:len(trainPredict)+look_back, :] = trainPredict
bktestPredictPlot = numpy.empty_like(dataset)
bktestPredictPlot[:, :] = numpy.nan
bktestPredictPlot[len(trainPredict)+(look_back*2)+1:len(dataset)-1, :] = bktestPredict
testPredictPlot = numpy.empty_like(dataset_with_test_predictions)
testPredictPlot[:,:] = numpy.nan
testPredictPlot[dataset.shape[0]+1:len(dataset_with_test_predictions)-1,:] = testPredict
dates = numpy.asarray(generate_dates(number_of_predictions, last_date, testPredict.shape[0]))
predictions_with_dates = defaultdict(list)
for i in range(0,len(testPredict)):
    predictions_with_dates["dates"].append(str(dates[i]))
    predictions_with_dates["predictions"].append(str(testPredict[i][0]))
path = os.path.join(algo,"output",cbu,phub,cmts,macdomain)
df = pd.DataFrame.from_dict(predictions_with_dates).to_csv('part-00000', index=False, header=False)
plt.plot(scaler.inverse_transform(dataset))
plt.plot(bktestPredictPlot)
plt.plot(testPredictPlot)
plt.show()
boto3.Session().resource('s3').Bucket("video-poc-test-case").Object(os.path.join(path,'_predict', 'part-00000')).upload_file('part-00000')


