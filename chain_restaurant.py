# Chain restaurants
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('Data/all_reviews_cleaned.csv')

# filter: Subset the dataframe rows or columns according to the specified index labels.
count_name = df.filter(['name'])

# Get number of restaurants for each business
count_name = count_name.groupby(count_name['name']).size().reset_index(name='count')
count_name = count_name.sort_values(by=['count'], ascending = False)

# Number of locations to be a chain restaurant
removed_outliers = count_name['count'] >= 10
count_name = count_name[removed_outliers]

plt.hist(count_name['count'], bins=10)
# plt.show()
# print(count_name)

join = pd.merge(df, count_name, on = 'name')
join = join.sort_values(by=['count'], ascending = False)
print(join)

# df.to_csv('chain_restaurant.csv')

