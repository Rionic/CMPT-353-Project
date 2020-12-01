import pandas as pd

df = pd.read_json('yelp-data/yelp_academic_dataset_business.json', lines=True)

df = df[df.categories.str.contains('Restaurants', regex=False, na=False)]
top_city = df.groupby(df.city).count()
top_city = top_city.sort_values(by=['business_id']).iloc[-1].name
df = df[df.city == top_city]


df.to_json('business_cleaned.csv')





