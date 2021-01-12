# dbt-ml-preprocessing

A package for dbt which enables standardization of data sets. You can use it to build a feature store in your data warehouse, without using external libraries like Spark's mllib or Python's scikit-learn.

The package contains a set of macros that mirror the functionality of the [scikit-learn preprocessing module](https://scikit-learn.org/stable/modules/preprocessing.html). Originally they were developed as part of the 2019 Medium article [Feature Engineering in Snowflake](https://medium.com/omnata/feature-engineering-in-snowflake-4312032e0d53).

Currently they have only been tested in Snowflake and Bigquery.

The macros are:

| scikit-learn function | macro name | Snowflake | BigQuery | Redshift |
| --- | --- | --- | --- | --- |
| [KBinsDiscretizer](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer)| k_bins_discretizer  | Y | Y | N |
| [LabelEncoder](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelEncoder.html#sklearn.preprocessing.LabelEncoder)| label_encoder  | Y | Y | N |
| [MaxAbsScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MaxAbsScaler.html#sklearn.preprocessing.MaxAbsScaler) | max_abs_scaler | Y | Y | N |
| [MinMaxScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html#sklearn.preprocessing.MinMaxScaler) | min_max_scaler | Y | Y | N |
| [Normalizer](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Normalizer.html#sklearn.preprocessing.Normalizer) | normalizer | Y | Y | N |
| [OneHotEncoder](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder) | one_hot_encoder | Y | Y | N |
| [QuantileTransformer](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.QuantileTransformer.html#sklearn.preprocessing.QuantileTransformer) | quantile_transformer | Y | N | N |
| [RobustScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.RobustScaler.html#sklearn.preprocessing.RobustScaler) | robust_scaler | Y | Y | N |
| [StandardScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html#sklearn.preprocessing.StandardScaler) | standard_scaler | Y | Y | N |

## Installation
To use this in your dbt project, create or modify packages.yml to include:
```
packages:
  - git: "https://github.com/omnata-pty-ltd/dbt-ml-preprocessing.git"
    revision: 0.2.0
```
_(replace the revision number with the latest)_

Then run:
```dbt deps``` to import the package.

## Usage
To read the macro documentation and see examples, simply run [generate your docs](https://docs.getdbt.com/reference/commands/cmd-docs/), and you'll see macro documentation in the Projects tree under ```dbt_ml_preprocessing```.


