import pandas as pd

# Getting data into a form we want
df = pd.read_csv('../2-cleaned_data/business_reviews_users_merged.csv')
df = df.drop(columns=['Unnamed: 0', 'user_id', 'business_name', 'is_open', 'business_stars', 'business_review_count', 'review_id', 'username', 'average_stars', 'date', 'elite', 'user_review_count'])
df = df.groupby(df['business_id']).mean()

df.to_csv('../2-cleaned_data/business_rating.csv')