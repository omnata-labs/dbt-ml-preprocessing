# dbt-ml-preprocessing

A plugin for dbt, to enable standardization of data sets.

The plugin contains a set of macros that mirror the functionality of the [scikit-learn preprocessing module](https://scikit-learn.org/stable/modules/preprocessing.html).

The macros are:

| Name | scikit-learn function | Snowflake |
| ---- | --------------------- |           |
| k_bins_discretizer | KBinsDiscretizer | yes |

