from sklearn.preprocessing import OneHotEncoder
import pandas as pd

test_dataset_df = pd.read_csv("data_one_hot_encoder.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = OneHotEncoder(handle_unknown='ignore').fit(second_column)

transformed_columns=transformer.transform(second_column).toarray()
print(transformed_columns)
test_dataset_df['is_column_to_encode_A']=transformed_columns[:, 0].astype(int)
test_dataset_df['is_column_to_encode_B']=transformed_columns[:, 1].astype(int)
test_dataset_df['is_column_to_encode_C']=transformed_columns[:, 2].astype(int)
test_dataset_df['is_column_to_encode_D']=transformed_columns[:, 3].astype(int)
test_dataset_df.to_csv("data_one_hot_encoder_expected.csv",index=False)
