import pandas as pd
import psycopg2
import sqlalchemy

def load_db(pathin, city, port):
    engine = sqlalchemy.create_engine(f'postgresql+psycopg2://etl_user:123@localhost:{port}/weather_db')
    try:
        df = pd.read_json(pathin) 
        df["city"] = city
        df.to_sql('weather_forecast', engine, if_exists='append', index=False)
    except Exception as e:
        print(e)
        return False
    return True