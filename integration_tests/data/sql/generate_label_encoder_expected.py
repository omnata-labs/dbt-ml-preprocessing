from sklearn.preprocessing import LabelEncoder
import pandas as pd

test_dataset_df = pd.read_csv("data_label_encoder.csv")
second_column = test_dataset_df.iloc[:, 1:2]
transformer = LabelEncoder()
transformer.fit(second_column)
test_dataset_df['col_to_label_labelled']=transformer.transform(second_column)
print(test_dataset_df)
test_dataset_df.to_csv("data_label_encoder_expected.csv",index=False)
