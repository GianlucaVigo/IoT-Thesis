import datetime

date1_str = "2010-10-06"
date1 = datetime.datetime.strptime(date1_str, "%Y-%m-%d").date()

date2_str = "2010-10-05"
date2 = datetime.datetime.strptime(date2_str, "%Y-%m-%d").date()

diff = date1 - date2

print(diff.days + 5)
print(type(diff.days))