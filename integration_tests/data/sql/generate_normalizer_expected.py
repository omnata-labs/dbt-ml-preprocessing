from sklearn.preprocessing import Normalizer
import pandas as pd

test_dataset_df = pd.read_csv("data_normalizer.csv")
second_to_fourth_columns = test_dataset_df.iloc[:, 1:5]
transformer = Normalizer().fit(second_to_fourth_columns) # fit does nothing.
transformer.fit(second_to_fourth_columns)
transformed_columns=transformer.transform(second_to_fourth_columns)
print(transformed_columns)
test_dataset_df['col1_normalized']=transformed_columns[:, 0]
test_dataset_df['col2_normalized']=transformed_columns[:, 1]
test_dataset_df['col3_normalized']=transformed_columns[:, 2]
test_dataset_df['col4_normalized']=transformed_columns[:, 3]
test_dataset_df.to_csv("data_normalizer_expected.csv",index=False)

