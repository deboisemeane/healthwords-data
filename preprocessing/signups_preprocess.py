from sklearn.preprocessing import OneHotEncoder, StandardScaler, RobustScaler
import pandas as pd

# Load raw data
df = pd.read_csv("data/raw/raw_signups.csv")

# One-hot encode the categorical traffic source
encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
encoded_traffic = encoder.fit_transform(df[['first_traffic_channel_group']])
encoded_traffic_df = pd.DataFrame(encoded_traffic, columns=encoder.get_feature_names_out(['first_traffic_channel_group']))

# Drop the original column and replace with encoded features
df = df.drop(columns=['first_traffic_channel_group', 'user_pseudo_id'])
df = pd.concat([df, encoded_traffic_df], axis=1)

# Standardise numerical columns
scaler = RobustScaler()
df[['total_sessions', 'total_engagement_sec', 'explore_page_views','shop_page_views']] = scaler.fit_transform(df[['total_sessions', 'total_engagement_sec', 'explore_page_views','shop_page_views']])

# Save preprocessed data to CSV
df.to_csv("data/preprocessed/preprocessed_signups.csv", index=False)
