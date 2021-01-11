from sklearn.preprocessing import QuantileTransformer
import pandas as pd
import numpy as np


# The following generates the sample data in the first instance
#rng = np.random.RandomState(0)
#X = np.sort(rng.normal(loc=0.5, scale=0.25, size=(300, 1)), axis=0)
#print(X)
#df = pd.DataFrame(X,columns=['col_to_transform'])
#df.to_csv("data_quantile_transformer.csv",index=True,index_label='id_col')

test_dataset_df = pd.read_csv("data_quantile_transformer.csv")
second_column = test_dataset_df.iloc[:, 1:2]

qt = QuantileTransformer(n_quantiles=10, random_state=0,output_distribution='uniform')
transformer = qt.fit_transform(second_column)
print(transformer)

test_dataset_df['col_to_transform_transformed']=transformer

test_dataset_df.to_csv("data_quantile_transformer_expected.csv",index=False)