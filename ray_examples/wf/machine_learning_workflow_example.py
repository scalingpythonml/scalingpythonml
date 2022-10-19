import ray
from ray import workflow

import pandas as pd
import numpy as np
from sklearn import base
from sklearn.base import BaseEstimator
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split

ray.init(address='auto')
workflow.init()

@ray.workflow.virtual_actor
class estimator_virtual_actor():
    def __init__(self, estimator: BaseEstimator):
        if estimator is not None:
            self.estimator = estimator

    def fit(self, inputtuple):
        (X, y, mode)= inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            self.estimator.fit(X, y)
            return X, y, mode
        else:
            X = self.estimator.fit_transform(X)
            return X, y, mode

    @workflow.virtual_actor.readonly
    def predict(self, inputtuple):
        (X, y, mode) = inputtuple
        if base.is_classifier(self.estimator) or base.is_regressor(self.estimator):
            pred_y = self.estimator.predict(X)
            return X, pred_y, mode
        else:
            X = self.estimator.transform(X)
            return X, y, mode

    def run_workflow_step(self, inputtuple):
        (X, y, mode) = inputtuple
        if mode == 'fit':
            return self.fit(inputtuple)
        elif mode == 'predict':
            return self.predict(inputtuple)

    def __getstate__(self):
        return self.estimator

    def __setstate__(self, estimator):
        self.estimator = estimator

## prepare the data
X = pd.DataFrame(np.random.randint(0,100,size=(10000, 4)), columns=list('ABCD'))
y = pd.DataFrame(np.random.randint(0,2,size=(10000, 1)), columns=['Label'])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

@workflow.step
def scaling(inputtuple, name):
    va = estimator_virtual_actor.get_or_create(name, StandardScaler())
    outputtuple = va.run_workflow_step.run_async(inputtuple)
    return outputtuple

@workflow.step
def classification(inputtuple, name):
    va = estimator_virtual_actor.get_or_create(name,
                                               DecisionTreeClassifier(max_depth=3))
    outputtuple = va.run_workflow_step.run_async(inputtuple)
    return outputtuple

training_tuple = (X_train, y_train, 'fit')
classification.step(scaling.step(training_tuple, 'standardscalar'),
                    'decisiontree').run('training_pipeline')

predict_tuple = (X_test, y_test, 'predict')
(X, pred_y, mode) = classification.step(scaling.step(predict_tuple, 'standardscalar'),
                                        'decisiontree').run('prediction_pipeline')
assert pred_y.shape[0] == 2000
