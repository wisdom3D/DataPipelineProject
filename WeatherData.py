import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import psycopg2
from sqlalchemy import create_engine

def getWeatherData(country, city, latitude, longitude):
   # Setup the Open-Meteo API client with cache and retry on error
  cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
  retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
  openmeteo = openmeteo_requests.Client(session = retry_session)

  # Make sure all required weather variables are listed here
  # The order of variables in hourly or daily is important to assign them correctly below
  url = "https://api.open-meteo.com/v1/forecast"
  params = {
    "latitude": latitude,
    "longitude": longitude,
    "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "weather_code", "surface_pressure", "cloud_cover_mid", "visibility", "evapotranspiration", "wind_speed_10m", "wind_direction_10m", "soil_temperature_6cm", "soil_moisture_1_to_3cm"],
    "timezone": "auto",
    #"past_days": 3,
    "forecast_days": 1
  }
  responses = openmeteo.weather_api(url, params=params)
  response = responses[0]
  # Process first location. Add a for-loop for multiple locations or weather models

  # Process hourly data. The order of variables needs to be the same as requested.
  hourly = response.Hourly()
  hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
  hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
  hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
  hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
  hourly_precipitation_probability = hourly.Variables(4).ValuesAsNumpy()
  hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
  hourly_rain = hourly.Variables(6).ValuesAsNumpy()
  hourly_weather_code = hourly.Variables(7).ValuesAsNumpy()
  hourly_surface_pressure = hourly.Variables(8).ValuesAsNumpy()
  hourly_cloud_cover_mid = hourly.Variables(9).ValuesAsNumpy()
  hourly_visibility = hourly.Variables(10).ValuesAsNumpy()
  hourly_evapotranspiration = hourly.Variables(11).ValuesAsNumpy()
  hourly_wind_speed_10m = hourly.Variables(12).ValuesAsNumpy()
  hourly_wind_direction_10m = hourly.Variables(13).ValuesAsNumpy()
  hourly_soil_temperature_6cm = hourly.Variables(14).ValuesAsNumpy()
  hourly_soil_moisture_1_to_3cm = hourly.Variables(15).ValuesAsNumpy()

  hourly_data = {"date": pd.date_range(
    start = pd.to_datetime(hourly.Time(), unit = "s"),
    end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
    freq = pd.Timedelta(seconds = hourly.Interval()),
    inclusive = "left"
  )}
  hourly_data["Pays"] = country
  hourly_data["Ville"] = city
  hourly_data["Latitude"] = response.Latitude()
  hourly_data["Longitude"] = response.Longitude()
  hourly_data["Température_2m"] = hourly_temperature_2m
  hourly_data["Humidité_relative_2m"] = hourly_relative_humidity_2m
  hourly_data["Point_de_rosée_2m"] = hourly_dew_point_2m
  hourly_data["Température_apparente"] = hourly_apparent_temperature
  hourly_data["Probabilité_de_précipitation"] = hourly_precipitation_probability
  hourly_data["Précipitations"] = hourly_precipitation
  hourly_data["Pluie"] = hourly_rain
  hourly_data["Code_météorologique"] = hourly_weather_code
  hourly_data["Pression_de_surface"] = hourly_surface_pressure
  hourly_data["Couverture_nuageuse_Moyenne"] = hourly_cloud_cover_mid
  hourly_data["Visibilité"] = hourly_visibility
  hourly_data["Évapotranspiration"] = hourly_evapotranspiration
  hourly_data["Vitesse_du_vent_10m"] = hourly_wind_speed_10m
  hourly_data["Direction_du_vent_10m"] = hourly_wind_direction_10m
  hourly_data["Température_du_sol_6cm"] = hourly_soil_temperature_6cm
  hourly_data["Humidité_du_sol_3cm"] = hourly_soil_moisture_1_to_3cm

  hourly_dataframe = pd.DataFrame(data = hourly_data)
  return hourly_dataframe

def transformData():
    PortoNovo = getWeatherData('Benin', 'Porto-Novo', 6.4965, 2.6036)
    Lome = getWeatherData('Togo', 'Lome', 6.1287, 1.2215)
    Natitingou = getWeatherData('Benin', 'Natitingou', 10.3042, 1.3796)
    Atakpame = getWeatherData('Togo', 'Atakpame', 7.5333, 1.1333)
    Bohicon = getWeatherData('Benin', 'Bohicon', 7.1783, 2.0667)
    Kara = getWeatherData('Togo', 'Kara', 9.5511, 1.1861)
    Tsevie = getWeatherData('Togo', 'Tsévié', 6.4261, 1.2133)
    Parakou = getWeatherData('Benin', 'Parakou', 9.3372, 2.6303)
    Cotonou = getWeatherData('Benin', 'Cotonou', 6.3654, 2.4183)
    Aneho = getWeatherData('Togo', 'Aneho', 6.228, 1.5919)
    Douala = getWeatherData('Cameroun', 'Douala', 4.0483, 9.7043)
    Ouidah = getWeatherData('Benin', 'Ouidah', 6.3631, 2.0851)
    Sokode = getWeatherData('Togo', 'Sokode', 8.9833, 1.1333)
    Dapaong = getWeatherData('Togo', 'Dapaong', 10.8622, 0.2076)
    PortGentil = getWeatherData('Gabon', 'Port-Gentil', 0.7193, 8.7815)
    Libreville = getWeatherData('Gabon', 'Libreville', 0.3924, 9.4536)
    Kpalime = getWeatherData('Togo', 'Kpalime', 6.9, 0.6333)
    data = pd.concat([PortoNovo, Lome, Natitingou, Atakpame, Bohicon, Kara, Tsevie,
                      Parakou, Cotonou, Aneho, Douala, Ouidah, Sokode, Dapaong, PortGentil, Libreville, Kpalime],
                     ignore_index=True)
    return  data
#print(transformData())
def loadData(data):
   # data = transformData()
    db_username = "postgres"
    db_password = "92913250"
    db_host = "localhost"
    db_port = "5432"
    db_name = "WeatherData"
    # conn = None
    # cur = None
    # Establish the database connection
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port
        )
    # conn.close()
    except Exception as error:
        pass
    # establish a connection to my database

    connection = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    # con = connection.connect()
    # load data into my database

    # data.to_sql(con = con, name = "WeatherTable", if_exists ="append", index = False)
    data.to_sql("WeatherTableB", connection, if_exists="append", index=False)

    # close the connection
    connection.dispose()
loadData(transformData())


'''SELECT "Pays", "Ville", sum("Humidité_relative_2m") Humidité_relative_2m, 
sum("Température_apparente") Température_apparente,
sum("Probabilité_de_précipitation") Probabilité_de_précipitation, 
sum("Précipitations") Précipitations
FROM public."WeatherTableB" 
group by "Pays", "Ville"
'''