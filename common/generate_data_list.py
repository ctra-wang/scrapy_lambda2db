from datetime import datetime, timedelta


# 生成整年年份数组 类似：date = ['2023-01-01', '2023-01-02']
def generate_date_list(year, month=None):
    if month is None:
        if year == datetime.now().year:
            end_date = datetime.now() - timedelta(days=1)  # 当前日期的前一天
        else:
            end_date = datetime(year, 12, 31)
        start_date = datetime(year, 1, 1)
    else:
        if year == datetime.now().year and month == datetime.now().month:
            end_date = datetime.now() - timedelta(days=1)  # 当前日期的前一天
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        start_date = datetime(year, month, 1)

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    print(date_list)
    return date_list
