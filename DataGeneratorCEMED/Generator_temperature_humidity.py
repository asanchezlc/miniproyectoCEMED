
from datetime import datetime
import calendar
import numpy as np
import sqlite3
import time
from threading import Thread, Event

"""
Temperature and Humidity Generator

Remarks:
    - There is a small issue to be corrected: the amplitude of the variability
    is minimum and maximum in min_var_month, min_var_month+6; but in the middle
    is almost zero. This shouldn't occur (it should be with absolute value, and intermediate
    in the middle of the year)
"""


class Temperature_Humidity_Generator:
    def __init__(self, fs, sensor_numbers_temperature, sensor_numbers_humidity, db_path,
                 T_var_year, T_aver_year, T_var_min_day, T_var_max_day,
                 hottest_hour, hottest_month, min_var_month, T_sigma_noise,
                 H_var_year, H_aver_year, H_var_min_day, H_var_max_day,
                 max_humidity_hour, max_humidity_month, min_var_humidity_month,
                 H_sigma_noise):
        self.fs = fs
        self.sensor_numbers_temperature = sensor_numbers_temperature
        self.sensor_numbers_humidity = sensor_numbers_humidity
        self.sensor_numbers = sensor_numbers_temperature + sensor_numbers_humidity
        self.db_path = db_path
        self.T_var_year = T_var_year
        self.T_aver_year = T_aver_year
        self.T_var_min_day = T_var_min_day
        self.T_var_max_day = T_var_max_day
        self.hottest_hour = hottest_hour
        self.hottest_month = hottest_month
        self.min_var_month = min_var_month
        self.T_sigma_noise = T_sigma_noise
        self.H_var_year = H_var_year
        self.H_aver_year = H_aver_year
        self.H_var_min_day = H_var_min_day
        self.H_var_max_day = H_var_max_day
        self.max_humidity_hour = max_humidity_hour
        self.max_humidity_month = max_humidity_month
        self.min_var_humidity_month = min_var_humidity_month
        self.H_sigma_noise = H_sigma_noise
        self.setup_temperature_humidity_db()
        self.stop_event = Event()
        self.db_thread = None
        self.stop_event = Event()

    def start(self):
        self.db_thread = Thread(target=self.execute_with_fs)
        self.db_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.db_thread:
            self.db_thread.join()

    def execute_with_fs(self):
        while not self.stop_event.is_set():
            self.save_to_db()
            time.sleep(1/self.fs)

    def generate_temperature(self, month, hour):
        T_year = self.T_var_year * \
            np.sin(np.pi*month/6 + (np.pi/2-np.pi *
                   self.hottest_month/6)) + self.T_aver_year
        T_day = (self.T_var_max_day + self.T_var_min_day)/2 + (self.T_var_max_day - self.T_var_min_day)/2 * \
            np.sin(np.pi*month/6-np.pi*(3+self.min_var_month)/6) * \
            np.sin(np.pi*(hour + 6-self.hottest_hour)/12)

        T = T_year + T_day
        return T

    def generate_humidity(self, month, hour):
        H_year = self.H_var_year * \
            np.sin(np.pi*month/6 + (np.pi/2-np.pi *
                   self.max_humidity_month/6)) + self.H_aver_year
        H_day = (self.H_var_max_day + self.H_var_min_day)/2 + (self.H_var_max_day - self.H_var_min_day)/2*np.sin(
            np.pi*month/6-np.pi*(3+self.min_var_humidity_month)/6) * np.sin(np.pi*(hour + 6-self.max_humidity_hour)/12)

        H = H_year + H_day
        return H

    def generate_temperature_data(self):
        # Generates data for all sensors and adds noise
        date = datetime.now()
        month, hour = self.get_fractional_month_and_hour(date)
        T = list()
        for _ in self.sensor_numbers_temperature:
            T_sensor = self.generate_temperature(
                month, hour) + np.random.normal(0, self.T_sigma_noise)
            T.append(T_sensor)

        return T

    def generate_humidity_data(self):
        # Generates data for all sensors, adds noise and limits value to 99.6
        date = datetime.now()
        month, hour = self.get_fractional_month_and_hour(date)
        H = list()
        for _ in self.sensor_numbers_humidity:
            H_sensor = self.generate_humidity(
                month, hour) + np.random.normal(0, self.H_sigma_noise)
            H_sensor = min(H_sensor, 99.6)
            H.append(H_sensor)

        return H

    def get_fractional_month_and_hour(self, date):
        # Calculate fractional month
        total_days_in_month = calendar.monthrange(date.year, date.month)[1]
        day_fraction = date.day / total_days_in_month
        fractional_month = date.month + day_fraction

        # Calculate fractional hour
        hour_fraction = (date.minute + date.second / 60) / 60
        fractional_hour = date.hour + hour_fraction

        return fractional_month, fractional_hour

    def setup_temperature_humidity_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS timestamps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_number INTEGER UNIQUE
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS temperature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_id INTEGER NOT NULL,
            sensor_id INTEGER NOT NULL,
            temperature REAL NOT NULL,
            FOREIGN KEY (timestamp_id) REFERENCES timestamps(id),
            FOREIGN KEY (sensor_id) REFERENCES sensors(id)
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS humidity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_id INTEGER NOT NULL,
            sensor_id INTEGER NOT NULL,
            humidity REAL NOT NULL,
            FOREIGN KEY (timestamp_id) REFERENCES timestamps(id),
            FOREIGN KEY (sensor_id) REFERENCES sensors(id)
        );
        ''')

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON timestamps(timestamp);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sensor_number ON sensors(sensor_number);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_temperature_timestamp_id ON temperature(timestamp_id);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_temperature_sensor_id ON temperature(sensor_id);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_humidity_timestamp_id ON humidity(timestamp_id);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_humidity_sensor_id ON humidity(sensor_id);")

        conn.commit()

        # Insert sensor numbers into the sensors table if they don't already exist
        for sensor_number in self.sensor_numbers:
            cursor.execute(
                'INSERT OR IGNORE INTO sensors (sensor_number) VALUES (?)', (sensor_number,))

        conn.commit()
        conn.close()

    def save_to_db(self):
        T_data = self.generate_temperature_data()
        H_data = self.generate_humidity_data()
        timestamp = time.time()

        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()

        # Fetch sensor IDs (sensor_ids[number] = id with that number)
        sensor_ids_temperature = {number: c.execute('SELECT id FROM sensors WHERE sensor_number = ?', (number,)).fetchone()[0]
                                  for number in self.sensor_numbers_temperature}
        sensor_ids_humidity = {number: c.execute('SELECT id FROM sensors WHERE sensor_number = ?', (number,)).fetchone()[0]
                               for number in self.sensor_numbers_humidity}

        c.execute('INSERT INTO timestamps (timestamp) VALUES (?)', (timestamp,))
        timestamp_id = c.lastrowid

        data_temperature, data_humidity = list(), list()
        for sensor_number, value in zip(self.sensor_numbers_temperature, T_data):
            sensor_db_id_temperature = sensor_ids_temperature[sensor_number]
            data_temperature.append(
                (timestamp_id, sensor_db_id_temperature, value))
        for sensor_number, value in zip(self.sensor_numbers_humidity, H_data):
            sensor_db_id_humidity = sensor_ids_humidity[sensor_number]
            data_humidity.append((timestamp_id, sensor_db_id_humidity, value))

        c.executemany(
            'INSERT INTO temperature (timestamp_id, sensor_id, temperature) VALUES (?, ?, ?)', data_temperature)
        c.executemany(
            'INSERT INTO humidity (timestamp_id, sensor_id, humidity) VALUES (?, ?, ?)', data_humidity)

        conn.commit()
        conn.close()


if __name__ == "__main__":
    """
    Data explanation for temperature (analogous for humidity):
    - T_var_year: amplitude of the temperature variability throughout the year
    - T_aver_year: average temperature throughout the year
    - T_var_min_day: minimum amplitude of the temperature variability throughout the day for the month of
        smaller variability
    - T_var_max_day: maximum amplitude of the temperature variability throughout the day for the month of
        higher variability
    - hottest_hour: hour of the day when the temperature is highest
    - hottest_month: month of the year when the temperature is highest
    - min_var_month: month of the year when the temperature variability is minimum
    - T_sigma_noise: standard deviation of the noise added to the temperature data
    """
    db_path = r'C:/xampp/htdocs/APIRestCEMED/sqldb/temperature_humidity.db'
    fs = 1/5  # Sampling frequency
    sensor_numbers_temperature = [1]  # list containing numbers of sensors
    sensor_numbers_humidity = [2]  # different numbers to sensor_numbers_temperature

    # Temperature data for generation
    T_var_year, T_aver_year = 10, 15
    T_var_min_day, T_var_max_day = 6, 9
    hottest_hour, hottest_month, min_var_month = 14, 7, 1
    T_sigma_noise = 0.5

    # Humidity data for generation
    H_var_year, H_aver_year = 18, 56
    H_var_min_day, H_var_max_day = 10, 20
    max_humidity_hour, max_humidity_month, min_var_humidity_month = 2, 1, 1
    H_sigma_noise = 3

    generator = Temperature_Humidity_Generator(fs, sensor_numbers_temperature, sensor_numbers_humidity, db_path,
                                               T_var_year, T_aver_year, T_var_min_day, T_var_max_day,
                                               hottest_hour, hottest_month, min_var_month, T_sigma_noise,
                                               H_var_year, H_aver_year, H_var_min_day, H_var_max_day,
                                               max_humidity_hour, max_humidity_month, min_var_humidity_month,
                                               H_sigma_noise)
    generator.start()
    input("Press Enter to stop...\n")
    generator.stop()
    print("Generator has been cleanly stopped.")
