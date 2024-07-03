from datetime import datetime
import numpy as np
import sqlite3
import time
from threading import Thread, Event
import queue


class DataGenerator:
    def __init__(self, sensor_numbers, fn, fs, run_time, number_of_sensors, buffer_size):
        self.sensor_numbers = sensor_numbers
        self.fn = fn
        self.fs = fs
        self.run_time = run_time
        self.start_time = time.time()
        self.stop_event = Event()
        self.data_queue = queue.Queue()
        self.db_path = r'C:/xampp/htdocs/APIRestCEMED/sqldb/accelerations.db'
        self.number_of_sensors = number_of_sensors
        self.buffer_size = buffer_size
        self.delta_f, self.mean_noise, self.std_noise, self.coef_f = self.generate_random_variables(
            fn, number_of_sensors)
        self.setup_accelerations_db()

    def start(self):
        self.db_thread = Thread(target=self.save_to_db)
        self.db_thread.start()
        self.data_thread = Thread(target=self.generate_data)
        self.data_thread.start()
        if self.run_time > 0:
            timer_thread = Thread(target=self.stop_after_duration)
            timer_thread.start()

    def generate_data(self):
        target_time = time.time()
        while not self.stop_event.is_set():
            target_time += 1 / self.fs
            while time.time() < target_time:
                pass  # Busy-wait to achieve the desired fs
            if self.run_time > 0 and (time.time() - self.start_time) > self.run_time:
                self.stop_event.set()
                break
            timestamp = time.time()  # Capture the current time for each data point
            data = [float(self.generate_sensor_data([timestamp], id_sensor)[0])
                    for id_sensor in range(self.number_of_sensors)]
            sensor_numbers_data = [self.sensor_numbers[i] for i in range(
                len(data))]  # generation of sensor numbers
            self.data_queue.put((timestamp, data, sensor_numbers_data))

    def save_to_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()
        batch_timestamps = []
        batch_accelerations = []

        # Fetch sensor IDs (sensor_ids[number] = id with that number)
        sensor_ids = {number: c.execute('SELECT id FROM sensors WHERE sensor_number = ?', (number,)).fetchone()[0]
                      for number in self.sensor_numbers[:self.number_of_sensors]}

        while not self.stop_event.is_set() or not self.data_queue.empty():
            if not self.data_queue.empty():
                timestamp, data, sensor_numbers = self.data_queue.get()
                batch_timestamps.append((timestamp,))
                c.execute(
                    'INSERT INTO timestamps (timestamp) VALUES (?)', (timestamp,))
                timestamp_id = c.lastrowid

                for sensor_number, value in zip(sensor_numbers, data):
                    sensor_db_id = sensor_ids[sensor_number]
                    batch_accelerations.append(
                        (timestamp_id, sensor_db_id, value))

                if len(batch_timestamps) >= self.buffer_size:
                    c.executemany(
                        'INSERT INTO timestamps (timestamp) VALUES (?)', batch_timestamps)
                    c.executemany(
                        'INSERT INTO accelerations (timestamp_id, sensor_id, acceleration_value) VALUES (?, ?, ?)', batch_accelerations)
                    conn.commit()
                    batch_timestamps = []
                    batch_accelerations = []

        conn.close()

    def stop(self):
        self.stop_event.set()

    def stop_after_duration(self):
        time.sleep(self.run_time)
        self.stop()

    def close(self):
        self.stop_event.set()
        if self.data_thread.is_alive():
            self.data_thread.join()
        if self.db_thread.is_alive():
            self.db_thread.join()

    def setup_accelerations_db(self):
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
        CREATE TABLE IF NOT EXISTS accelerations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_id INTEGER NOT NULL,
            sensor_id INTEGER NOT NULL,
            acceleration_value REAL NOT NULL,
            FOREIGN KEY (timestamp_id) REFERENCES timestamps(id),
            FOREIGN KEY (sensor_id) REFERENCES sensors(id)
        );
        ''')

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON timestamps(timestamp);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sensor_number ON sensors(sensor_number);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp_id ON accelerations(timestamp_id);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sensor_id ON accelerations(sensor_id);")

        conn.commit()

        # Insert sensor names into the sensors table if they don't already exist
        for sensor_number in self.sensor_numbers:
            cursor.execute(
                'INSERT OR IGNORE INTO sensors (sensor_number) VALUES (?)', (sensor_number,))

        conn.commit()
        conn.close()

    def generate_random_variables(self, fn, number_of_sensors):
        np.random.seed(0)
        delta_f = [i*0.006 for i in fn]
        mean_noise = np.random.uniform(0, 0.03, number_of_sensors)
        std_noise = np.random.uniform(0, 0.01, number_of_sensors)
        coef_f = np.random.uniform(
            10**-3, 10**-2, (number_of_sensors, len(fn)))
        return delta_f, mean_noise, std_noise, coef_f

    def generate_freq(self, hour, delta_f, fn, h_max_freq=4):
        hour = np.array(hour)
        if hour.ndim == 0:
            hour = np.array([hour])
        freq_h = fn + delta_f*np.cos(2*np.pi*(hour - h_max_freq)/24)
        std_dev = 0  # we finally not disturbe the frequency with which generate
        freq_h += np.random.normal(0, std_dev, len(hour))
        freq_h = list(freq_h)
        return freq_h

    def generate_sensor_data(self, timestamps, id_sensor, h_max_freq=4):
        t = np.array(timestamps)
        n_modes = len(self.fn)
        x_noise = np.random.normal(
            self.mean_noise[id_sensor], self.std_noise[id_sensor], len(t))
        x_armonics = np.zeros(len(t))
        h = np.array([datetime.fromtimestamp(i).hour for i in t])
        for i in range(n_modes):
            freq = np.array(self.generate_freq(
                h, self.delta_f[i], self.fn[i], h_max_freq=h_max_freq))
            x_armonics += np.sin(2*np.pi*freq*t)*self.coef_f[id_sensor][i]
        return x_noise + x_armonics


if __name__ == "__main__":
    fn = [3.773, 4.988, 5.609, 7.949]  # from OMA (Enrique)
    fs = 1  # Hz
    run_time = 0  # Run indefinitely until stopped (in seconds)
    number_of_sensors = 6
    sensor_numbers = [_ for _ in range(1, number_of_sensors+1)]
    buffer_size = 20  # Number of records to save to the database at once

    generator = DataGenerator(sensor_numbers, fn, fs,
                              run_time, number_of_sensors, buffer_size)
    generator.start()
    input("Press Enter to stop...\n")
    generator.close()
    print("Generator has been cleanly stopped.")
