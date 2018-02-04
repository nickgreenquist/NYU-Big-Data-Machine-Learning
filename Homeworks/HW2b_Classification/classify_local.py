from sklearn.datasets import fetch_20newsgroups
import numpy as np
import re
from nltk.corpus import stopwords

from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.multiclass import OneVsOneClassifier
from sklearn.svm import LinearSVC
import sklearn.svm
from sklearn import metrics

# Narrow down categories to use
categories = [
    'alt.atheism',
    'comp.graphics',
    'comp.os.ms-windows.misc',
    'comp.sys.ibm.pc.hardware',
    'comp.sys.mac.hardware',
    'comp.windows.x',
    'misc.forsale',
    'rec.autos',
    'rec.motorcycles',
    'rec.sport.baseball',
    'rec.sport.hockey',
    'sci.crypt',
    'sci.electronics',
    'sci.med',
    'sci.space',
    'soc.religion.christian',
    'talk.politics.guns',
    'talk.politics.mideast',
    'talk.politics.misc',
    'talk.religion.misc'
]

def loadData(data_type):
    print("Loading 20 newsgroups " + data_type + " dataset for categories:")
    print(categories)

    if data_type == 'train':
        data = fetch_20newsgroups(subset='train', remove=('headers', 'footers', 'quotes'), categories=categories)
    else:
        data = fetch_20newsgroups(subset='test', remove=('headers', 'footers', 'quotes'), categories=categories)
    print("%d documents" % len(data.filenames))
    print("%d categories" % len(data.target_names))
    print()

    return data

def preprocessor(data):
    cleaned = []
    for d in data:
        d = re.sub("[^a-zA-Z]", " ", d)
        cleaned.append(d)
    return cleaned

def classify_MNB(train, test):
    print("Multinomial Naive Bayes")
    text_clf = Pipeline([
        ('vect', CountVectorizer(stop_words='english')),
        ('tfidf', TfidfTransformer()),
        ('clf', MultinomialNB()),
    ])

    text_clf.fit(train.data, train.target)  
    predicted = text_clf.predict(test.data)
    results = np.mean(predicted == test.target)  
    print("Basic Accuracy: " + str(results))
    print(metrics.classification_report(test.target, predicted, target_names=test.target_names))

def classify_SGD(train, test):
    print("SGD")
    text_clf = Pipeline([
        ('vect', CountVectorizer(stop_words='english')),
        ('tfidf', TfidfTransformer()),
        ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, random_state=42, max_iter=5, tol=None)),
    ])
    text_clf.fit(train.data, train.target)  

    predicted = text_clf.predict(test.data)
    results = np.mean(predicted == test.target) 
    print("Basic Accuracy: " + str(results))
    print(metrics.classification_report(test.target, predicted, target_names=test.target_names))

def classify_OneVsOne(train, test):
    print ("OneVsOneClassifier")
    text_clf = Pipeline([
        ('vectorizer', CountVectorizer()),
        ('tfidf', TfidfTransformer()),
        ('clf', OneVsOneClassifier(LinearSVC()))])
    text_clf.fit(train.data, train.target)  

    predicted = text_clf.predict(test.data)
    results = np.mean(predicted == test.target) 
    print("Basic Accuracy: " + str(results))
    print(metrics.classification_report(test.target, predicted, target_names=test.target_names))

def main():
    train = loadData("train")
    test = loadData("test")

    train.data = preprocessor(train.data)
    test.data = preprocessor(test.data)

    classify_MNB(train, test)
    classify_SGD(train, test)
    classify_OneVsOne(train, test)

#MAIN --  do not put code below this
main()

