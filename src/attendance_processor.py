import pandas as pd
from datetime import datetime, timedelta

def read_and_prepare_data(file_path):
    try:
        data = pd.read_csv(file_path, parse_dates=['Date'])
        data['Clock-in Time'] = pd.to_datetime(data['Clock-in Time'], format='%H:%M:%S').dt.time
        data['Clock-out Time'] = pd.to_datetime(data['Clock-out Time'], format='%H:%M:%S').dt.time
        return data
    except Exception as e:
        print(f"Error reading or preparing the data: {e}")
        return None

def calculate_hours_worked(clock_in, clock_out):
    if not clock_in or not clock_out:
        return 0, 0
    fulldate = datetime(1, 1, 1)
    dt_clock_in = datetime.combine(fulldate, clock_in)
    dt_clock_out = datetime.combine(fulldate, clock_out)
    total_hours = (dt_clock_out - dt_clock_in).total_seconds() / 3600
    overtime_hours = max(total_hours - 8, 0) if total_hours > 8 else 0
    print("calculate_hours_worked output:", total_hours, overtime_hours)  # Debugging: Print the output
    return total_hours, overtime_hours

def process_biweekly_report(df, start_date, end_date):
    biweekly_data = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].copy()

    standard_start = datetime.strptime('09:00:00', '%H:%M:%S').time()
    standard_end = datetime.strptime('17:00:00', '%H:%M:%S').time()

    # Calculate total hours and overtime hours
    results = biweekly_data.apply(
        lambda row: calculate_hours_worked(row['Clock-in Time'], row['Clock-out Time']), axis=1, result_type='expand'
    )
    
    # Using .loc to avoid SettingWithCopyWarning
    biweekly_data.loc[:, 'Total Hours'] = results.iloc[:, 0]
    biweekly_data.loc[:, 'Overtime Hours'] = results.iloc[:, 1]

    # For Late Clock-ins and Early Departures
    biweekly_data.loc[:, 'Late Clock-ins'] = biweekly_data['Clock-in Time'].apply(
        lambda x: x > standard_start if pd.notnull(x) else False
    )
    biweekly_data.loc[:, 'Early Departures'] = biweekly_data['Clock-out Time'].apply(
        lambda x: x < standard_end if pd.notnull(x) else False
    )

    summary = biweekly_data.groupby('Employee ID').agg(
        Total_Hours_Worked=pd.NamedAgg(column='Total Hours', aggfunc='sum'),
        Total_Overtime=pd.NamedAgg(column='Overtime Hours', aggfunc='sum'),
        Late_Clock_Ins=pd.NamedAgg(column='Late Clock-ins', aggfunc='sum'),
        Early_Departures=pd.NamedAgg(column='Early Departures', aggfunc='sum')
    ).reset_index()

    return summary




def generate_biweekly_periods(start_date, end_date):
    period_start = start_date
    while period_start < end_date:
        period_end = period_start + timedelta(days=13)
        yield period_start, min(period_end, end_date)
        period_start = period_end + timedelta(days=1)

def main():
    file_path = 'data/raw/mock_attendance_data.csv'
    report_path = 'data/reports/'
    data = read_and_prepare_data(file_path)

    if data is not None:
        # Adjust the date range to match the dataset
        report_start_date = datetime(2024, 1, 1)
        report_end_date = datetime(2024, 1, 31)  # Example end date

        for start_date, end_date in generate_biweekly_periods(report_start_date, report_end_date):
            biweekly_report = process_biweekly_report(data, start_date, end_date)
            report_file_name = f'{report_path}biweekly_report_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.csv'
            biweekly_report.to_csv(report_file_name, index=False)

if __name__ == "__main__":
    main()
