from sklearn.preprocessing import KBinsDiscretizer
import pandas as pd
import numpy as np

# The following generates the sample data in the first instance
X = np.random.randint(low=-100, high=100, size=(300, 2))
print(X)
df = pd.DataFrame(X,columns=['col_to_bin_1','col_to_bin_2'])
df.to_csv("data_k_bins_discretizer.csv",index=True,index_label='id_col')

test_dataset_df = pd.read_csv("data_k_bins_discretizer.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = KBinsDiscretizer(n_bins=[20], encode='ordinal',strategy='uniform').fit(second_column)
test_dataset_binned = transformer.fit_transform(second_column)
test_dataset_df['col_to_bin_1_binned']=test_dataset_binned
third_column = test_dataset_df.iloc[:, 2:3]
transformer = KBinsDiscretizer(n_bins=[20], encode='ordinal',strategy='uniform').fit(third_column)
test_dataset_binned = transformer.fit_transform(third_column)
test_dataset_df['col_to_bin_2_binned']=test_dataset_binned
test_dataset_df.to_csv("data_k_bins_discretizer_default_bins_expected.csv",index=False)

# Generate a single column using 50 bins
test_dataset_df = pd.read_csv("data_k_bins_discretizer.csv")
transformer = KBinsDiscretizer(n_bins=[50], encode='ordinal',strategy='uniform').fit(second_column)
test_dataset_binned = transformer.fit_transform(second_column)
test_dataset_df['col_to_bin_1_binned']=test_dataset_binned
test_dataset_df.to_csv("data_k_bins_discretizer_50_bins_expected.csv",index=False)
