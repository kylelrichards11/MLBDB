# MLB DB
This purpose of this project is to provide an easy way to access and manipulate Major League Baseball data to then feed into machine learning models. Currently, data from 2017-2020 is available. This project is still in the beginning stages and is not ready to be used.

# Getting Started
Because of the size, the statcast data is not in this repo. To obtain it run `python scrapers.py`. This will save all of the statcast data from 2017-2020 as feather files. 

# Getting Features
Using the methods of the `FeatureMaker` class you can supplement the statcast features with additional features. Because dask is used for DataFrame manipulation, make sure you call the `compute()` method on your final DataFrame.