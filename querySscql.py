import http.client
import json
import mysql.connector
import time
from datetime import datetime, timedelta

from common import db_config

# Mapping of JSON keys to database columns
key_to_column = {
    "实时市场出清总电量": "true_time_market_clearing_total_power",
    "实时市场调频里程价格": "true_time_market_roads_price",
    "实时市场出清电能价格": "true_time_market_clearing_power_price",
    "实时市场出清网损价格": "true_time_market_clearing_losses_price",
    "实时市场出清节点电价": "true_time_market_clearing_node_price",
    "实时市场出清阻塞价格": "true_time_market_clearing_block_price",
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
    conn = http.client.HTTPSConnection("lambda.com.cn")
    payload = json.dumps({
        "device": "甘肃.光辉变/330kV.330kVⅠ母",
        "province": "甘肃省",
        "time": today
    })
    headers = {
        'Authorization': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJiY2wiLCJleHAiOjE2OTI3MDcyNjIsImxvZ2luVHlwZSI6MCwiY3JlYXRlZCI6MTY5MjY3MTI2MjczMywiYXV0aG9yaXRpZXMiOlt7ImF1dGhvcml0eSI6InN5czp1c2VyOnZpZXcifSx7ImF1dGhvcml0eSI6InN5czp1c2VyOmVkaXQifSx7ImF1dGhvcml0eSI6InN5czp1c2VyOmRlbGV0ZSJ9LHsiYXV0aG9yaXR5Ijoic3lzOnVzZXI6YWRkIn1dfQ.53WWousGu5KrQ0wDvShCs1KF7Nh44iEjS4EzNEhmozhEW6hnVf4yj1gPOsCnvrRqwZV_4B8-_LmbY4Ft2WVyww',
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': 'lambda.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=A82C767CB3F5AB892271B8CB9BDD3CF2'
    }
    conn.request("POST", "/gansu/basic/queryData/sscql/querySscql", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

    res = {}
    json_data = json.loads(data.decode("utf-8"))

    print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;')
    for item in json_data["data"]:
        if item["name"] == "实时市场出清总电量":
            res = item["data"]

        # Connect to the MySQL database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        arr = res.split(",")

        # Generate and execute the INSERT INTO statement for each index
        for i in range(len(arr)):
            values = [0, 0, 0, 0, 0, 0]
            for item in json_data["data"]:
                if item["name"] == "实时市场出清总电量":
                    values[0] = item["data"].split(',')[i]
                if item["name"] == "实时市场调频里程价格":
                    values[1] = item["data"].split(',')[i]
                if item["name"] == "实时市场出清电能价格":
                    values[2] = item["data"].split(',')[i]
                if item["name"] == "实时市场出清网损价格":
                    values[3] = item["data"].split(',')[i]
                if item["name"] == "实时市场出清节点电价":
                    values[4] = item["data"].split(',')[i]
                if item["name"] == "实时市场出清阻塞价格":
                    values[5] = item["data"].split(',')[i]

            # Additional fixed values for the new columns
            date_value = today
            province_value = "gansu"
            device = "甘肃.光辉变/330kV.330kVⅠ母"
            # # moment_values = generate_time_values()
            #
            # # Append additional column values to the end of the values list

            values.extend([date_value, province_value, moment_values[i], device])
            # print("---------------")
            # print(i)
            # print(values)

            placeholders = ', '.join(['%s'] * len(values))
            columns = ', '.join([key for key in key_to_column.values()] + ["date", "province", "moment", "device"])
            print(columns)
            insert_query = f"INSERT INTO lambda_querySscql ({columns}) VALUES ({placeholders})"

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
    date = ['2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06', '2023-01-07',
            '2023-01-08',
            '2023-01-09', '2023-01-10', '2023-01-11', '2023-01-12', '2023-01-13', '2023-01-14', '2023-01-15',
            '2023-01-16',
            '2023-01-17', '2023-01-18', '2023-01-19', '2023-01-20', '2023-01-21', '2023-01-22', '2023-01-23',
            '2023-01-24',
            '2023-01-25', '2023-01-26', '2023-01-27', '2023-01-28', '2023-01-29', '2023-01-30', '2023-01-31',
            '2023-02-01',
            '2023-02-02', '2023-02-03', '2023-02-04', '2023-02-05', '2023-02-06', '2023-02-07', '2023-02-08',
            '2023-02-09',
            '2023-02-10', '2023-02-11', '2023-02-12', '2023-02-13', '2023-02-14', '2023-02-15', '2023-02-16',
            '2023-02-17',
            '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22', '2023-02-23', '2023-02-24',
            '2023-02-25',
            '2023-02-26', '2023-02-27', '2023-02-28', '2023-03-01', '2023-03-02', '2023-03-03', '2023-03-04',
            '2023-03-05',
            '2023-03-06', '2023-03-07', '2023-03-08', '2023-03-09', '2023-03-10', '2023-03-11', '2023-03-12',
            '2023-03-13',
            '2023-03-14', '2023-03-15', '2023-03-16', '2023-03-17', '2023-03-18', '2023-03-19', '2023-03-20',
            '2023-03-21',
            '2023-03-22', '2023-03-23', '2023-03-24', '2023-03-25', '2023-03-26', '2023-03-27', '2023-03-28',
            '2023-03-29',
            '2023-03-30', '2023-03-31', '2023-04-01', '2023-04-02', '2023-04-03', '2023-04-04', '2023-04-05',
            '2023-04-06',
            '2023-04-07', '2023-04-08', '2023-04-09', '2023-04-10', '2023-04-11', '2023-04-12', '2023-04-13',
            '2023-04-14',
            '2023-04-15', '2023-04-16', '2023-04-17', '2023-04-18', '2023-04-19', '2023-04-20', '2023-04-21',
            '2023-04-22',
            '2023-04-23', '2023-04-24', '2023-04-25', '2023-04-26', '2023-04-27', '2023-04-28', '2023-04-29',
            '2023-04-30',
            '2023-05-01', '2023-05-02', '2023-05-03', '2023-05-04', '2023-05-05', '2023-05-06', '2023-05-07',
            '2023-05-08',
            '2023-05-09', '2023-05-10', '2023-05-11', '2023-05-12', '2023-05-13', '2023-05-14', '2023-05-15',
            '2023-05-16',
            '2023-05-17', '2023-05-18', '2023-05-19', '2023-05-20', '2023-05-21', '2023-05-22', '2023-05-23',
            '2023-05-24',
            '2023-05-25', '2023-05-26', '2023-05-27', '2023-05-28', '2023-05-29', '2023-05-30', '2023-05-31',
            '2023-06-01',
            '2023-06-02', '2023-06-03', '2023-06-04', '2023-06-05', '2023-06-06', '2023-06-07', '2023-06-08',
            '2023-06-09',
            '2023-06-10', '2023-06-11', '2023-06-12', '2023-06-13', '2023-06-14', '2023-06-15', '2023-06-16',
            '2023-06-17',
            '2023-06-18', '2023-06-19', '2023-06-20', '2023-06-21', '2023-06-22', '2023-06-23', '2023-06-24',
            '2023-06-25',
            '2023-06-26', '2023-06-27', '2023-06-28', '2023-06-29', '2023-06-30', '2023-07-01', '2023-07-02',
            '2023-07-03',
            '2023-07-04', '2023-07-05', '2023-07-06', '2023-07-07', '2023-07-08', '2023-07-09', '2023-07-10',
            '2023-07-11',
            '2023-07-12', '2023-07-13', '2023-07-14', '2023-07-15', '2023-07-16', '2023-07-17', '2023-07-18',
            '2023-07-19',
            '2023-07-20', '2023-07-21', '2023-07-22', '2023-07-23', '2023-07-24', '2023-07-25', '2023-07-26',
            '2023-07-27',
            '2023-07-28', '2023-07-29', '2023-07-30', '2023-07-31', '2023-08-01', '2023-08-02', '2023-08-03',
            '2023-08-04',
            '2023-08-05', '2023-08-06', '2023-08-07', '2023-08-08', '2023-08-09', '2023-08-10', '2023-08-11',
            '2023-08-12',
            '2023-08-13', '2023-08-14', '2023-08-15', '2023-08-16', '2023-08-17', '2023-08-18', '2023-08-19',
            '2023-08-20', '2023-08-21']

    # print_hi("2023-01-01")

    for item in date:
        print('当前执行日期为：', item)
        print_hi(item)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
