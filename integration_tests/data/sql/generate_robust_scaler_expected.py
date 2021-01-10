from sklearn.preprocessing import RobustScaler
import pandas as pd

test_dataset_df = pd.read_csv("data_robust_scaler.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = RobustScaler(with_centering=False).fit(second_column)
test_dataset_df['col_to_scale_scaled']=transformer.transform(second_column)
print(test_dataset_df)
test_dataset_df.to_csv("data_robust_scaler_expected.csv",index=False)
