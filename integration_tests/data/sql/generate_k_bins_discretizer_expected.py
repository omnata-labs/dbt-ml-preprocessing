from sklearn.preprocessing import KBinsDiscretizer
import pandas as pd

test_dataset_df = pd.read_csv("data_k_bins_discretizer.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = KBinsDiscretizer(n_bins=[20], encode='ordinal',strategy='uniform').fit(second_column)
test_dataset_binned = transformer.fit_transform(second_column)
test_dataset_df['col_to_bin_binned']=test_dataset_binned
test_dataset_df.to_csv("data_k_bins_discretizer_default_bins_expected.csv",index=False)

transformer = KBinsDiscretizer(n_bins=[50], encode='ordinal',strategy='uniform').fit(second_column)
test_dataset_binned = transformer.fit_transform(second_column)
test_dataset_df['col_to_bin_binned']=test_dataset_binned
test_dataset_df.to_csv("data_k_bins_discretizer_50_bins_expected.csv",index=False)
