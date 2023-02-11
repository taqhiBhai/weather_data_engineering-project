from airflow.decorators import task,dag
import requests
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import time

@dag(
    dag_id='weather_etl',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 0o1, 0o1),
    catchup=False
)

def weather_etl():
    query = ""
    create_schema = PostgresOperator(
        task_id = 'create_schema',
        sql = """CREATE SCHEMA IF NOT EXISTS forecast;""",
        postgres_conn_id="post_local_windows"
    )
    create_table = PostgresOperator(
        task_id = 'create_table',
        sql = """CREATE TABLE IF NOT EXISTS forecast.weather (
                    id serial PRIMARY KEY,
                    city varchar,
                    celsius varchar,
                    fahrenheit varchar,
                    humidity varchar,
                    country varchar,
                    sky varchar,
                    time timestamp
                    );""",
        # postgres connector
        postgres_conn_id="post_local_windows"
    )

    create_schema >> create_table

    @task()
    def weather():
        # Create an empty list to store the weather data for each city.
        weather_data = []
        api_key = ('api_key')
        def kevin_cel_to_far(kelvin):
            celsius = kelvin - 273.15
            fahrenheit = celsius * (9/5) + 32
            return celsius, fahrenheit

        cities = ['hyderabad','karimnagar','warangal']
        ## specifing url
        for i in cities:
            url = 'https://api.openweathermap.org/data/2.5/weather?&appid='+api_key+"&q={}".format(i)
            print(url)

            ## web request
            r = requests.get(url).json()
            print(r)
            
            temp_kelvin = r['main']['temp']
            hum = r['main']['humidity']
            country = r['sys']['country']
            temp_celsius,temp_fahrenheit = kevin_cel_to_far(temp_kelvin)
            sky = r['weather'][0]['description']
            # gettting time
            curr_time = time.localtime()
            conv_time = time.strftime("%d/%m/%Y, %H:%M:%S", curr_time)

            city_data = {
                'City': i,
                'Celsius': f"{round(temp_celsius)}°C",
                'Fahrenheit': f"{round(temp_fahrenheit)}°F",
                'Humidity': hum,
                'Country': country,
                'Sky': sky,
                'Time': conv_time
            }
            # Add the dictionary to the list of weather data.
            weather_data.append(city_data)
        # Return the list of weather data.
        return weather_data
    
    weather_data = weather()
    create_table.set_downstream(weather_data)

    @task()
    def load_data(get_weather_data):
        hook = PostgresHook(postgres_conn_id="post_local_windows")
        for data in get_weather_data:
            values = list(data.values())
            real_data = [values]
            print(real_data)
            hook.insert_rows(table='forecast.weather',rows=real_data,target_fields=['city', 'celsius', 'fahrenheit', 'humidity', 'country', 'sky', 'time'])

    load_data(weather_data)

weather_etl_airflow = weather_etl()