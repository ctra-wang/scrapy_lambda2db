import http.client
import json
import mysql.connector
import time
from datetime import datetime, timedelta

from common.db_config import DB_CONFIG
from common.generate_data_list import generate_date_list
from common.lambda_token import LAMBDA_TOKEN
from common.points import MOMENT_VALUES

# Replace these with your MySQL database credentials


# Mapping of JSON keys to database columns
key_to_column = {
    "新能源实时出力(合计)": "new_energy_power_total",
    "联络线潮流（合计）": "call_wire_flow_total",
    "新能源实时出力(风电)": "new_energy_power_wind",
    "新能源实时出力(光电)": "new_energy_power_photoelectricity",
    "联络线输电曲线预测（日前合计）": "call_wire_curve_forecast_dayahead",
    "火电开机总出力": "thermal_power_turn_on_power",
    "联络线输电曲线预测（日内合计）": "call_wire_curve_forecast_intraday",
    "实际总出力": "real_total_power",
    "水电实时总出力": "water_power_true_time_power",
    "实际负荷数据": "real_loading",
    "非市场化机组实际出力": "no_marketization_unit_real_power",
}

# Function to generate time values from "00:15" to "24:00" in 15-minute increments
# Define the start and end times
start_time = time.strptime('00:15', '%H:%M')
end_time = time.strptime('23:59', '%H:%M')


def print_hi(today, device):
    conn = http.client.HTTPSConnection("lambda.com.cn")
    payload = json.dumps({
        "device": device,
        "province": "甘肃省",
        "time": today
    })
    headers = {
        'Authorization': LAMBDA_TOKEN,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': 'lambda.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=A82C767CB3F5AB892271B8CB9BDD3CF2'
    }
    conn.request("POST", "/gansu/basic/queryData/rqcql/queryRqcql", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

    res = {}
    json_data = json.loads(data.decode("utf-8"))
    for item in json_data["data"]:
        if item["name"] == "日前市场出清节点电价":
            res = item["data"]

        # Connect to the MySQL database
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        arr = res.split(",")
        print(arr)
        # Generate and execute the INSERT INTO statement for each index
        for i in range(len(arr)):
            # Extract values from JSON based on the same index
            # values = [json_data["data"][key].split(",")[i] for key in key_to_column.values()]

            # Additional fixed values for the new columns
            date_value = today
            province_value = "gansu"
            values = [arr[i]]
            values.extend([date_value, province_value, MOMENT_VALUES[i], device])
            # # Prepare placeholders for the values

            placeholders = ', '.join(['%s'] * len(values))
            columns = ', '.join(["dayahead_price", "date", "province", "moment", "device"])
            # print(values)
            # print(columns)
            insert_query = f"INSERT INTO lambda_queryRqcql ({columns}) VALUES ({placeholders})"

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
    dateList = generate_date_list(2023)
    # 甘肃.新庄变/330kV.330kVⅠ母
    device = "甘肃.光辉变/330kV.330kVⅠ母"
    # date = ["2023-07-29", "2023-05-28", "2023-05-21", "2023-05-20", "2023-05-18", "2023-05-16", "2023-05-14",
    #         "2023-05-09", "2023-05-07", "2023-05-06", "2023-05-05", "2023-04-14", "2023-04-04", "2023-04-03"]
    for item in dateList:
        print('当前执行日期为：', item)
        print_hi(item, device)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
