"""Using Pandas to work with databases"""
import sqlite3
import pandas as pd

def iter_records(csv_file):
    df = pd.read_csv(csv_file)
    #print(df.head(30))

    #Date string -> date type
    #print(df.dtypes)
    df['DATE'] = df['DATE'].apply(str)
    df['DATE'] = pd.to_datetime(df['DATE'], format="%Y%M%d")

    #for date in df['DATE']:
    #print(df.head(100))
    #print(df.dtypes)
    #def date_change(current_date):

    #tmin(cel/10) -> min_temp(float, farenheit)
    #tmax(cel/10) -> max_temp(float, farenheit)
    df = df.assign(TMIN_CONVERT = (df['TMIN']*10 * 9/5) + 32)
    df = df.assign(TMAX_CONVERT = (df['TMAX']*10 * 9/5) + 32)
    #print(df.head(30))
    #print(df.isna().any().any())
    #show(int) ->snow(integer)

    #deal with na values
    df1 = df[['DATE','TMIN_CONVERT','TMAX_CONVERT','SNOW']]
    df1.dropna()
    #print(df[['DATE','TMIN_CONVERT','TMAX_CONVERT','SNOW']])

    #turn df into tuples
    for r in df1.columns.values:
        df1[r] = df1[r].map(str)
        df1[r] = df1[r].map(str.strip)
        tuples = [tuple(x) for x in df1.values]

    return tuples

#queries
sql = 'SELECT distance FROM rides WHERE vendor = :vendor'
insert_sql = '''INSERT INTO weather (day, min_temp, max_temp, snow)
    VALUES (?,?,?,?)'''


def etl(csv_file, db_file):
    with sqlite3.connect(db_file) as db:
        cur = db.cursor()
        with open('schema.sql') as fp:
            cur.executescript(fp.read())
        cur.executemany(insert_sql, iter_records(csv_file))
        return cur.rowcount

if __name__ == '__main__':
    count = etl('weather.csv', 'weather.db')
    print(f'inserted {count} records')
