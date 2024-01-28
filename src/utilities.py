import pandas as pd
import random
from datetime import datetime, timedelta

def generate_work_hours():
    clock_in_hour = random.randint(8, 9)
    clock_in_minute = random.randint(0, 59)
    clock_in = datetime.strptime(f"{clock_in_hour}:{clock_in_minute}:00", '%H:%M:%S').time()

    clock_out_hour = clock_in_hour + 8 + random.randint(0, 1)
    clock_out_minute = (clock_in_minute + random.randint(0, 30)) % 60
    clock_out = datetime.strptime(f"{clock_out_hour}:{clock_out_minute}:00", '%H:%M:%S').time()

    return clock_in, clock_out

def generate_mock_attendance_data(num_employees=3, num_days=60, start_date=datetime(2024, 1, 1)):
    employee_ids = [f"E_{i:03d}" for i in range(1, num_employees + 1)]
    dates = [start_date + timedelta(days=i) for i in range(num_days)]

    data = []
    for employee_id in employee_ids:
        for date in dates:
            clock_in, clock_out = generate_work_hours()
            data.append([employee_id, date.date(), clock_in, clock_out])

    attendance_df = pd.DataFrame(data, columns=["Employee ID", "Date", "Clock-in Time", "Clock-out Time"])
    return attendance_df

def save_mock_data_to_csv(df, file_path):
    df.to_csv(file_path, index=False)
