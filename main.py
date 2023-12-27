import pandas as pd


def bar_race(data):
    grouped_df = data.groupby('Year').agg(
        {'North America': 'sum', 'Europe': 'sum', 'Japan': 'sum', 'Rest of World': 'sum'}).reset_index()
    grouped_df.columns = ['Year', 'North America', 'Europe', 'Japan', 'Rest of World']
    grouped_df[['North America', 'Europe', 'Japan', 'Rest of World']] = grouped_df[
        ['North America', 'Europe', 'Japan', 'Rest of World']].cumsum()
    transposed_df = grouped_df.transpose()
    transposed_df.to_csv('bar_race.csv', index=False, header=True)


def increase_2008_to_2012(data):
    grouped_df = data.groupby('Year').agg(
        {'North America': 'sum', 'Europe': 'sum', 'Japan': 'sum', 'Rest of World': 'sum',
         'Global': 'sum'}).reset_index()
    grouped_df.columns = ['Year', 'North America', 'Europe', 'Japan', 'Rest of World', 'Global']
    no_cum_sum = data.groupby('Year').agg(
        {'North America': 'sum', 'Europe': 'sum', 'Japan': 'sum', 'Rest of World': 'sum',
         'Global': 'sum'}).reset_index()
    no_cum_sum.columns = ['Year', 'North America', 'Europe', 'Japan', 'Rest of World', 'Global']
    grouped_df[['North America', 'Europe', 'Japan', 'Rest of World', 'Global']] = grouped_df[
        ['North America', 'Europe', 'Japan', 'Rest of World', 'Global']].cumsum()
    no_cum_sum.to_csv('grouped.csv', index=False, header=True)


def proportion_sales_units_by_year(data):
    result = data.groupby('Year').agg(
        {'North America': 'mean', 'Europe': 'mean', 'Japan': 'mean', 'Rest of World': 'mean',
         'Global': 'mean'}).reset_index()
    result.columns = ['Year', 'North America', 'Europe', 'Japan', 'Rest of World', 'Global']
    result.to_csv('proportion.csv', index=False, header=True)
    print(result)


def average_sales_by_day(data):
    global_average_day_sales = data['Global'].sum() / 10957 * 1000000
    north_america_average_day_sales = data['North America'].sum() / 10957 * 1000000
    north_america_average_day_sales = data['North America'].sum() / 10957 * 1000000
    europe_average_day_sales = data['Europe'].sum() / 10957 * 1000000
    japan_average_day_sales = data['Japan'].sum() / 10957 * 1000000


def sales_per_genre(data):
    result = data.groupby('Genre').agg(Global_sum=('Global', 'sum'), Global_avg=('Global', 'mean')).reset_index()
    result = result.sort_values(by='Global_sum', ascending=False)
    result.to_csv('genre_sum.csv', index=False, header=True)
    result = result.sort_values(by='Global_avg', ascending=False)
    result.to_csv('genre_avg.csv', index=False, header=True)
    print(result)


def sales_per_platform(data):
    data['Platform Group'] = data['Platform'].where(df.groupby('Platform')['Platform'].transform('count') >= 50, 'Others')
    result = data.groupby('Platform Group').agg(Global_sum=('Global', 'sum'), Global_avg=('Global', 'mean')).reset_index()
    result = result.sort_values(by='Global_sum', ascending=False)
    result.to_csv('platform_sum.csv', index=False, header=True)
    result = result.sort_values(by='Global_avg', ascending=False)
    result.to_csv('platform_avg.csv', index=False, header=True)
    print(result)


def sales_per_publisher(data):
    data['Publisher Group'] = data['Publisher'].where(df.groupby('Publisher')['Publisher'].transform('count') >= 32, 'Others')
    result = data.groupby('Publisher Group').agg(Global_sum=('Global', 'sum'), Global_avg=('Global', 'mean')).reset_index()
    result = result.sort_values(by='Global_sum', ascending=False)
    result.to_csv('publisher_sum.csv', index=False, header=True)
    result = result.sort_values(by='Global_avg', ascending=False)
    result.to_csv('publisher_avg.csv', index=False, header=True)
    print(result)


def releases_by_year(data):
    result = data.groupby('Year').agg(Units=('Year', 'count')).reset_index()
    result = result.sort_values(by='Year', ascending=True)
    result.to_csv('releases.csv', index=False, header=True)
    print(result)


def releases_by_platform(data):
    data['Platform Group'] = data['Platform'].where(df.groupby('Platform')['Platform'].transform('count') >= 50, 'Others')
    result = data.groupby('Platform Group').agg(Units=('Platform Group', 'count')).reset_index()
    result = result.sort_values(by='Units', ascending=False)
    result.to_csv('releases_by_platform.csv', index=False, header=True)
    print(result)


def releases_by_publisher(data):
    data['Publisher Group'] = data['Publisher'].where(df.groupby('Publisher')['Publisher'].transform('count') >= 32, 'Others')
    result = data.groupby('Publisher Group').agg(Units=('Publisher Group', 'count')).reset_index()
    result = result.sort_values(by='Units', ascending=False)
    result.to_csv('releases_by_publisher.csv', index=False, header=True)
    print(result)


if __name__ == '__main__':
    file_name = 'video_game_sales.csv'
    df = pd.read_csv(file_name)
    df = df.dropna(subset=['Year'])
    df['Year'] = df['Year'].astype(int)
    bar_race(df)
    increase_2008_to_2012(df)
    proportion_sales_units_by_year(df)
    average_sales_by_day(df)
    sales_per_genre(df)
    releases_by_year(df)
    releases_by_platform(df)
    releases_by_publisher(df)
    sales_per_platform(df)
    sales_per_publisher(df)
