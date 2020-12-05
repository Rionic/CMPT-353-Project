#Chain restaurants
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('Data/business_reviews_users_merged.csv')

# filter: Subset the dataframe rows or columns according to the specified index labels.
count_name = df.filter(['business_name'])

# Get number of restaurants for each business
count_name = count_name.groupby(count_name['business_name']).size().reset_index(name='count')
count_name = count_name.sort_values(by=['count'], ascending = False)

# Number of locations to be a chain restaurant
removed_outliers = count_name['count'] >= 10 
count_name = count_name[removed_outliers]

# plt.hist(count_name['count'], bins=100)
# plt.show()
# print(count_name)

join = pd.merge(df, count_name, on = 'business_name')
join = join.sort_values(by=['count'], ascending = False)
print(join)

# df.to_csv('chain_restaurant.csv')
