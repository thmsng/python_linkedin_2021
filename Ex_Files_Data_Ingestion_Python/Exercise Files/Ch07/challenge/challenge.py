import sqlite3
import pandas as pd

#load db
def load_db(db_file):
    conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES)

    sql = 'SELECT * FROM rides'

    df = pd.read_sql(sql, conn)
    #print(df.dtypes)

    conn.close()

    return df

def clean_data(df):

    # add column with ride duration = drop off - pickup
    df = df.assign(ride_duration = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']))
    #print(df.dtypes)
    #print(df.head(20))

    # remove all row with duration <= minute
    df = df.drop(df[df['ride_duration']  <= pd.Timedelta(1,'m')].index)
    #print(df)

    # change all duration that are more than 5 hours to median duration
    mask = df['ride_duration'] > pd.Timedelta(5,'h')
    print('num outliers:', len(df[mask]))  # 7

    fill_value = df['ride_duration'].median()
    print('fill_value =', fill_value)  # 19.48

    df.loc[mask, 'ride_duration'] = fill_value
    print('max after fix', df['ride_duration'].max())  # 35.57

    print("===================")
    print(df.head(10))


    return df

if __name__ == '__main__':
    result = clean_data(load_db('rides.db'))
