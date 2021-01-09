from sklearn.preprocessing import MinMaxScaler
import pandas as pd

test_dataset_df = pd.read_csv("data_min_max_scaler.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = MinMaxScaler().fit(second_column)
test_dataset_df['col_to_scale_scaled']=transformer.transform(second_column)
print(test_dataset_df)
test_dataset_df.to_csv("data_min_max_scaler_expected.csv",index=False)
test_dataset_df=test_dataset_df.drop(columns=['col_to_scale'])
test_dataset_df.to_csv("data_min_max_scaler_with_column_selection_expected.csv",index=False)
