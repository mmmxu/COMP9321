# -*- coding: utf-8 -*-
"""9321-assn3.ipynb
"""  # Start"""


# ref: https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
import pandas as pd
import numpy as np
import requests
import json
import collections
import sys
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import BaggingClassifier, AdaBoostClassifier, RandomForestClassifier, GradientBoostingClassifier

from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from math import sqrt
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
pd.options.mode.chained_assignment = None  # default='warn'

# read file
# get data from args
if len(sys.argv) != 3:
    sys.exit("Input args not valid, please pass path1 path2 to this file.")

TRAIN_PATH = sys.argv[1]
VALID_PATH = sys.argv[2]
Z_ID = 'z5141401'
DECIMAL_LEN = 2

try:
    train_df = pd.read_csv(TRAIN_PATH)
    validation_df = pd.read_csv(VALID_PATH)
except:
    sys.exit("Invalid path or not a csv file, please check your input.")
# train_df

"""## Processing data


|Column|Value  |
|--|--|
|Budget|keep value, not convert|
|Genres|convert to number of genres|
|Homepage|convert to boolean (has a homepage or not)|
|original_language|keep???|
|release_date|convert to year|


[release_year_rita.png](https://i.loli.net/2020/04/16/Ul1LeRQtr6PDM3d.png)

[release_year_me](https://pic.luoxufeiyan.com/uploads/20200416145052.png)
"""

# Only keep certain columns as training set

# keep_col = ['budget', 'homepage', 'original_language', 'release_date']
# conv_num_list = ['cast', 'crew', 'genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages']
keep_col = ['budget', 'homepage', 'release_date',
            'genres', 'production_companies']
conv_num_list = ['crew']
Y_label = 'revenue'

"""### conv funcs"""

# convert-to num


def conv_to_num(dataset, modify_col):
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        curr_len = len(json.loads(curr_str))
        dataset[modify_col][i] = curr_len
    return dataset

# preprocessing-homepage


def proc_homepage(dataset):
    modify_col = 'homepage'
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # convert to Ture if movie has a homepage
        curr_len = (type(curr_str) is str)
        dataset[modify_col][i] = curr_len
    return dataset

# preprocessing-release_date


def proc_rls_year(dataset):
    modify_col = 'release_date'
    dataset['release_year'] = dataset[modify_col].copy()
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # convert to year only
        curr_len = int(curr_str[:4])
        dataset['release_year'][i] = curr_len

    return dataset

# preprocessing-release_month


def proc_rls_mo(dataset):
    modify_col = 'release_date'
    dataset['release_month'] = dataset[modify_col].copy()
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # convert to month only
        curr_len = int(curr_str[5:7])
        dataset['release_month'][i] = curr_len
    return dataset

# preprocessing-release_weeks


def proc_rls_weeks(dataset):
    dataset['release_week'] = dataset['release_date'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').isocalendar()[1])
    return dataset

# preprocessing-release_days of week


def proc_rls_dow(dataset):
    dataset['release_weekday'] = dataset['release_date'].apply(
        lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%A'))
    return dataset

# get main genre


def proc_main_genre(dataset):
    modify_col = 'genres'
    dataset['main_genre'] = dataset[modify_col].copy()
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # get first genre
        try:
            j1 = json.loads(curr_str)
            curr_len = j1[0]['name']
            dataset['main_genre'][i] = curr_len
        except:
            dataset['main_genre'][i] = 'Null'
    return dataset

# get main company id


def proc_main_comp_id(dataset):
    modify_col = 'production_companies'
    dataset['main_comp_id'] = dataset[modify_col].copy()
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # get first prod company
        j1 = json.loads(curr_str)
        try:
            curr_len = j1[0]['id']
            dataset['main_comp_id'][i] = curr_len
        except:
            dataset['main_comp_id'][i] = 0
    return dataset

# get main company id


def proc_budget_year_ratio(dataset):
    modify_col = 'budget'
    dataset['budget_year_ratio'] = dataset[modify_col].copy()
    for i in range(len(dataset[modify_col])):
        # calc ratio
        ratio = round(dataset[modify_col][i] / dataset['release_year'][i], 2)
        dataset['budget_year_ratio'][i] = ratio
    return dataset

# original_language


def proc_orig_lang(dataset):
    all_lang = list(set(dataset['original_language'].values))
    le = LabelEncoder()
    le.fit(all_lang)
    dataset['original_language'] = le.transform(dataset['original_language'])
    return dataset

# budget


def proc_norm(dataset, modify_col):

    # norm certain cols

    y_df = dataset[modify_col]
    y_norm = (y_df - y_df.min()) / (y_df.max() - y_df.min())
    dataset[modify_col] = y_norm
    return dataset

# inverse norm


def inverse_norm(orig_col, proc_col):
    # orginal col contains raw data
    inverse_col = proc_col * (orig_col.max() - orig_col.min()) + orig_col.min()
    return inverse_col

# count the number of possible returns per variable


def total_count(dataset, modify_col, topk=30):
    every_mention = []
    for i in range(len(dataset[modify_col])):
        curr_str = dataset[modify_col][i]
        # get genres string
        j1 = json.loads(curr_str)
        try:
            curr_str = train_df['genres'][i]
            j1 = json.loads(curr_str)
            names = map(lambda datum: datum['name'], j1)
            every_mention.append(names.values())
        except:
            pass
    print(every_mention)
    total = collections.Counter(every_mention).most_common(topk)
    return total


def proc_data(dataset):
    # add Y label
    selected_col = keep_col + conv_num_list
    selected_col.append(Y_label)
    proc_dataset = dataset[selected_col]
    # proc_dataset = dataset[keep_col]

    # do preprocessing
    # convert to num
    for i in conv_num_list:
        try:
            proc_dataset = conv_to_num(proc_dataset, i)
        except:
            pass

    # other processing
    proc_dataset = proc_main_genre(proc_dataset)
    proc_dataset = proc_main_comp_id(proc_dataset)
    proc_dataset = proc_rls_year(proc_dataset)
    proc_dataset = proc_rls_weeks(proc_dataset)
    proc_dataset = proc_rls_mo(proc_dataset)
    proc_dataset = proc_rls_dow(proc_dataset)
    proc_dataset = proc_homepage(proc_dataset)
    # calc after release year
    proc_dataset = proc_budget_year_ratio(proc_dataset)
    proc_dataset = proc_norm(proc_dataset, 'budget')

    # norm certain cols
    proc_dataset = proc_norm(proc_dataset, Y_label)

    # one hog
    # one-hot encode genres
    # one_hot = proc_dataset['main_genre'].str.get_dummies(sep=',')
    # # one-hot encode month variables
    # one_hot_month = pd.get_dummies(proc_dataset['release_month'], prefix='month')
    # # one-hot-encode weekday variable
    one_hot_weekday = pd.get_dummies(
        proc_dataset['release_weekday'], prefix='weekday')
    # # # one-hot-encode weeks variable
    # one_hot_weeks = pd.get_dummies(proc_dataset['release_week'], prefix='week')

    # proc_dataset = proc_dataset.join(one_hot)
    # proc_dataset = proc_dataset.join(one_hot_month)
    proc_dataset = proc_dataset.join(one_hot_weekday)
    # proc_dataset = proc_dataset.join(one_hot_weeks)

    # remove original rows
    remove_rows = ['main_comp_id', 'release_date', 'genres', 'production_companies',
                   'main_genre', 'release_month', 'release_weekday', 'budget']
    proc_dataset = proc_dataset.drop(remove_rows, axis=1)
    return proc_dataset

# def proc_data(dataset):
#     # add Y label
#     selected_col = keep_col + conv_num_list
#     selected_col.append(Y_label)
#     proc_dataset = dataset[selected_col]
#     # proc_dataset = dataset[keep_col]

#     # do preprocessing
#     #convert to num
#     for i in conv_num_list:
#         try:
#             proc_dataset = conv_to_num(proc_dataset, i)
#         except:
#             pass

#     # other processing
#     proc_dataset = proc_norm(proc_dataset, 'budget')
#     proc_dataset = proc_homepage(proc_dataset)
#     proc_dataset = proc_rls_date(proc_dataset)

#     #norm certain cols
#     proc_dataset = proc_norm(proc_dataset, Y_label)
#     return proc_dataset


"""#### norm

https://stackoverflow.com/questions/33246316/normalization-in-sci-kit-learn-linear-models

https://stackoverflow.com/questions/44552031/sklearnstandardscaler-can-i-inverse-the-standardscaler-for-the-model-output

### process data
"""

train_set = proc_data(train_df)
vali_set = proc_data(validation_df)


train_set

# total_count(train_df, 'genres')
# # curr_str = train_df['genres'][2]
# # j1 = json.loads(curr_str)
# # names = map(lambda datum: datum['name'], j1)
# # list(names)

"""# Fit models

* https://www.pluralsight.com/guides/non-linear-regression-trees-scikit-learn

* https://github.com/wangye707/PY_DELL/blob/b066ae4bb40273b24d698d66450e4207e3eb6808/git_book-master/chapters/Decision_Tree/decisiontree_regressor.py
"""

keep_col = list(train_set.columns.values)
keep_col.remove(Y_label)

X_train = train_set[keep_col]
Y_train = train_set[Y_label]


X_valid = vali_set[keep_col]
Y_valid = vali_set[Y_label]


"""### decision tree"""


def dTree(X_train, Y_train):
    model_dtree = DecisionTreeRegressor()
    # dtree = DecisionTreeRegressor()
    model_dtree.fit(X_train, Y_train)

    return model_dtree


"""### random forest"""


def randomForest(X_train, Y_train):
    # RF model
    model_rf = RandomForestRegressor(
        n_estimators=500, oob_score=True, random_state=100)
    # model_rf = RandomForestRegressor(n_estimators=500)
    model_rf.fit(X_train, Y_train)

    return model_rf


"""### Adaboost"""


def adaBoost(X_train, Y_train):
    model_ab = AdaBoostRegressor(n_estimators=500)
    model_ab.fit(X_train, Y_train)

    return model_ab


"""### GBRT"""


def gbrt(X_train, Y_train):
    model_gbrt = GradientBoostingRegressor(n_estimators=500)
    model_gbrt.fit(X_train, Y_train)

    return model_gbrt


"""# Evaluate"""

model_gbrt = gbrt(X_train, Y_train)
model = model_gbrt

# summary output
lst = [Z_ID]
summary_1_filename = Z_ID + '.PART1.summary.csv'
Y_valid_pred = model.predict(X_valid)
lst.append(round(mean_squared_error(Y_valid, Y_valid_pred), DECIMAL_LEN))
lst.append(round(pearsonr(Y_valid, Y_valid_pred)[0], DECIMAL_LEN))
print(lst)
summary_1 = pd.DataFrame([lst], columns=['zid', 'MSR', 'correlation'])
summary_1.to_csv(summary_1_filename, index=False)

# detail outputs
output_1_filename = Z_ID + '.PART1.output.csv'

outputs_pred = pd.DataFrame(Y_valid_pred)
outputs_inverse = validation_df.copy()
outputs_inverse['predicted_revenue'] = inverse_norm(
    validation_df[Y_label], outputs_pred)

outputs_1 = pd.DataFrame(outputs_inverse, columns=[
                         'movie_id', 'predicted_revenue'])
outputs_1.to_csv(output_1_filename, index=False)

# outputs_final_1
# # validation_df['movie_id']
# %pwd

"""# P2

https://blog.csdn.net/u010900574/article/details/52669072
"""

# Only keep certain columns as training set

# keep_col = ['budget', 'homepage', 'original_language', 'release_date']
# conv_num_list = ['cast', 'crew', 'genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages']

keep_col = ['budget', 'homepage', 'release_date',
            'genres', 'production_companies']
conv_num_list = ['crew']
Y_label = 'rating'

"""### proc data

Not norm Y-label
"""


def proc_data(dataset):
    # add Y label
    selected_col = keep_col + conv_num_list
    selected_col.append(Y_label)
    proc_dataset = dataset[selected_col]
    # proc_dataset = dataset[keep_col]

    # do preprocessing
    # convert to num
    for i in conv_num_list:
        try:
            proc_dataset = conv_to_num(proc_dataset, i)
        except:
            pass

    # other processing
    proc_dataset = proc_main_genre(proc_dataset)
    proc_dataset = proc_main_comp_id(proc_dataset)
    proc_dataset = proc_rls_year(proc_dataset)
    proc_dataset = proc_rls_weeks(proc_dataset)
    proc_dataset = proc_rls_mo(proc_dataset)
    proc_dataset = proc_rls_dow(proc_dataset)
    proc_dataset = proc_homepage(proc_dataset)
    # calc after release year
    proc_dataset = proc_budget_year_ratio(proc_dataset)
    proc_dataset = proc_norm(proc_dataset, 'budget')

    # #norm certain cols FOR P2 NOT NORM
    # proc_dataset = proc_norm(proc_dataset, Y_label)

    # one hog
    # one-hot encode genres
    # one_hot = proc_dataset['main_genre'].str.get_dummies(sep=',')
    # # one-hot encode month variables
    # one_hot_month = pd.get_dummies(proc_dataset['release_month'], prefix='month')
    # # one-hot-encode weekday variable
    one_hot_weekday = pd.get_dummies(
        proc_dataset['release_weekday'], prefix='weekday')
    # # # one-hot-encode weeks variable
    # one_hot_weeks = pd.get_dummies(proc_dataset['release_week'], prefix='week')

    # proc_dataset = proc_dataset.join(one_hot)
    # proc_dataset = proc_dataset.join(one_hot_month)
    proc_dataset = proc_dataset.join(one_hot_weekday)
    # proc_dataset = proc_dataset.join(one_hot_weeks)

    # remove original rows
    remove_rows = ['main_comp_id', 'release_date', 'genres', 'production_companies',
                   'main_genre', 'release_month', 'release_weekday', 'budget']
    proc_dataset = proc_dataset.drop(remove_rows, axis=1)
    return proc_dataset


train_set = proc_data(train_df)
vali_set = proc_data(validation_df)
train_set

keep_col = list(train_set.columns.values)
keep_col.remove(Y_label)

X_train = train_set[keep_col]
Y_train = train_set[Y_label]

X_valid = vali_set[keep_col]
Y_valid = vali_set[Y_label]

X_train

"""### SVM"""

clf = GradientBoostingClassifier(
    n_estimators=50, learning_rate=1.0, max_depth=1, random_state=10)
clf.fit(X_train, Y_train)
Y_valid_pred = clf.predict(X_valid)

# summary output
lst = [Z_ID]
summary_2_filename = Z_ID + '.PART2.summary.csv'
rating_labels = [1, 2, 3]

lst.append(round(precision_score(
    Y_valid, Y_valid_pred, average='macro'), DECIMAL_LEN))
lst.append(round(recall_score(Y_valid, Y_valid_pred, average='macro'), DECIMAL_LEN))
lst.append(round(accuracy_score(Y_valid, Y_valid_pred), DECIMAL_LEN))
print(lst)
summary_2 = pd.DataFrame(
    [lst], columns=['zid', 'average_precision', 'average_recall', 'accuracy'])
summary_2.to_csv(summary_2_filename, index=False)

# detail outputs
output_2_filename = Z_ID + '.PART2.output.csv'

outputs_pred = pd.DataFrame(Y_valid_pred)
outputs_inverse = validation_df.copy()
outputs_inverse['predicted_rating'] = inverse_norm(
    validation_df[Y_label], outputs_pred)

outputs_2 = pd.DataFrame(outputs_inverse, columns=[
                         'movie_id', 'predicted_rating'])
outputs_2.to_csv(output_2_filename, index=False)

# # Test different models

# from sklearn import tree, svm, naive_bayes,neighbors
# from sklearn.ensemble import BaggingClassifier, AdaBoostClassifier, RandomForestClassifier, GradientBoostingClassifier


# clfs = {'svm': svm.SVC(),\
#         'decision_tree':tree.DecisionTreeClassifier(),
#         'naive_gaussian': naive_bayes.GaussianNB(), \
#         # 'naive_mul':naive_bayes.MultinomialNB(),\
#         'K_neighbor' : neighbors.KNeighborsClassifier(),\
#         'bagging_knn' : BaggingClassifier(neighbors.KNeighborsClassifier(), max_samples=0.5,max_features=0.5), \
#         'bagging_tree': BaggingClassifier(tree.DecisionTreeClassifier(), max_samples=0.5,max_features=0.5),
#         'random_forest' : RandomForestClassifier(n_estimators=50),\
#         'adaboost':AdaBoostClassifier(n_estimators=50),\
#         'gradient_boost' : GradientBoostingClassifier(n_estimators=50, learning_rate=1.0,max_depth=1, random_state=10)
#         }

# def try_different_method(clf):
#     clf.fit(X_train,Y_train)
#     score = clf.score(X_valid,Y_valid)
#     print('the score is :', score)

# for clf_key in clfs.keys():
#     print('the classifier is :',clf_key)
#     clf = clfs[clf_key]
#     try_different_method(clf)
