import http.client
import json
import mysql.connector
import time
from datetime import datetime, timedelta

from common import db_config
from common.lambda_token import LAMBDA_TOKEN



# Mapping of JSON keys to database columns
key_to_column = {
    '发电总出力预测': "power_gen_total_power_forecast",
    '新能源总出力预测(光电)': "new_energy_total_power_forcast_photoelectricity",
    '新能源总出力预测(合计)': "new_energy_total_power_forcast_total",
    '新能源总出力预测(风电)': "new_energy_total_power_forcast_wind",
    '日前系统负荷预测(短期)': "dayahead_sys_loading_forecast_short",
    '日前系统负荷预测(超短期)': "dayahead_sys_loading_forecast_ultra_short",
    '日前系统间联络线总输电曲线预测(日内计划)': "dayahead_call_wire_curve_forecast_intraday",
    '日前系统间联络线总输电曲线预测(日前计划)': "dayahead_call_wire_curve_forecast_dayahead",
    '水电计划发电总出力预测': "water_power_plan_total_power_forecast",
    # '新能源总出力预测（日前披露）': "new_energy_total_power_forcast_dayahead",
}

# Function to generate time values from "00:15" to "24:00" in 15-minute increments
# Define the start and end times
start_time = time.strptime('00:15', '%H:%M')
end_time = time.strptime('23:59', '%H:%M')

# Generate moment values at 15-minute intervals as strings
moment_values = ['00:15', '00:30', '00:45', '01:00', '01:15', '01:30', '01:45', '02:00', '02:15', '02:30', '02:45',
                 '03:00', '03:15', '03:30', '03:45', '04:00', '04:15', '04:30', '04:45', '05:00', '05:15', '05:30',
                 '05:45', '06:00', '06:15', '06:30', '06:45', '07:00', '07:15', '07:30', '07:45', '08:00', '08:15',
                 '08:30', '08:45', '09:00', '09:15', '09:30', '09:45', '10:00', '10:15', '10:30', '10:45', '11:00',
                 '11:15', '11:30', '11:45', '12:00', '12:15', '12:30', '12:45', '13:00', '13:15', '13:30', '13:45',
                 '14:00', '14:15', '14:30', '14:45', '15:00', '15:15', '15:30', '15:45', '16:00', '16:15', '16:30',
                 '16:45', '17:00', '17:15', '17:30', '17:45', '18:00', '18:15', '18:30', '18:45', '19:00', '19:15',
                 '19:30', '19:45', '20:00', '20:15', '20:30', '20:45', '21:00', '21:15', '21:30', '21:45', '22:00',
                 '22:15', '22:30', '22:45', '23:00', '23:15', '23:30', '23:45', '24:00']


def print_hi(today):
    # headers = {"Content-Type": "application/json"}
    headers = {
        'Authorization': LAMBDA_TOKEN,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': 'lambda.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=BF4DE3D727BB10A4E0B5DB62DE3AE0F9'
    }
    # url = "https://lambda.com.cn/gansu/basic/queryData/xxpl"
    conn = http.client.HTTPSConnection("lambda.com.cn")
    payload = json.dumps({
        "province": "甘肃省",
        "time": today
    })
    # [57-61]
    # payload = {"province": "甘肃省", "data": "2023-01-01"}
    # response = requests.post(url, data=json.dumps(payload), headers=headers)
    conn.request("POST", "/gansu/basic/queryData/sqggxx", payload, headers)
    res = conn.getresponse()
    data = res.read()
    # print(data.decode("utf-8"))

    json_data = json.loads(data.decode("utf-8"))

    # Connect to the MySQL database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Get the number of values in the JSON data
        num_values = len(json_data["data"]["发电总出力预测"].split(","))

        # Generate and execute the INSERT INTO statement for each index
        for i in range(num_values):
            # Extract values from JSON based on the same index
            # values = [json_data["data"][key].split(",")[i] for key in key_to_column.values()]
            values = [json_data["data"][key].split(",")[i] for key in key_to_column]
            # Additional fixed values for the new columns
            date_value = today
            province_value = "gansu"
            # moment_values = generate_time_values()

            # Append additional column values to the end of the values list
            values.extend([date_value, province_value, moment_values[i]])
            # Prepare placeholders for the values
            placeholders = ', '.join(['%s'] * len(values))

            columns = ', '.join([key for key in key_to_column.values()] + ["date", "province", "moment"])

            insert_query = f"INSERT INTO lambda_sqggxx ({columns}) VALUES ({placeholders})"

            # Execute the INSERT INTO statement
            cursor.execute(insert_query, values)
            conn.commit()

        print("Data successfully inserted into the database.")

    except mysql.connector.Error as err:
        print("Error: ", err)

    finally:
        cursor.close()
        conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    date = ['2023-08-24']

    # date = ["2023-07-29", "2023-05-28", "2023-05-21", "2023-05-20", "2023-05-18", "2023-05-16", "2023-05-14",
    #         "2023-05-09", "2023-05-07", "2023-05-06", "2023-05-05", "2023-04-14", "2023-04-04", "2023-04-03"]
    #
    for item in date:
        print('当前执行日期为：', item)
        print_hi(item)
        # print_hi("2023-01-01")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
