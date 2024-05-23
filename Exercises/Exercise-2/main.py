import io
import requests
import pandas as pd


def retrieve_page(url, timeout):
    """Retrieve page content and store as DataFrame"""
    try:
        r = requests.get(url, timeout=timeout).text
        page_content_df = pd.read_html(io.StringIO(r))[0]    
    except requests.exceptions.HTTPError as e:
        print(e)
    return page_content_df


def retrieve_file(url, file_name, timeout):
    """Retrieve file and write locally"""
    r = requests.get(url, timeout=timeout)
    with open(file_name, "wb") as f:
        f.write(r.content)


def main():
    
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    file_timestamp = "2024-01-19 10:13" # Modified timestamp as original one doesn't exist...

    page_df = retrieve_page(url, 10)
    csv_file = page_df[page_df['Last modified'] == file_timestamp]['Name'].iloc[0]
    file_url = url + csv_file
    
    retrieve_file(file_url, csv_file, 10)
    weather_data = pd.read_csv(csv_file)

    hourly_dry_bulb_temperature_max = max(weather_data['HourlyDryBulbTemperature'])
    max_records = weather_data[weather_data['HourlyDryBulbTemperature'] >= hourly_dry_bulb_temperature_max]
    for column in max_records.columns:
        print(f"{column}: {max_records[column].iloc[0]}")


if __name__ == "__main__":
    main()
