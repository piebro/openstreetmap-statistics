import re
import sys
import json
import util

data_dir = sys.argv[1]

with open("assets/data.js", "r") as f:
    datajs_str = "".join(f.readlines())

months, _ = util.get_months_years(data_dir)


def replace_months(m):
    return f'"x": months.slice({len(months)-len(m.group().split(","))})'


datajs_str = re.sub(r"\"x\":\[(\"\d{4}-\d{2}\",?)*\]", replace_months, datajs_str)

# def replace_zeros(z):
#     return f'"y":[...Array({z.group().count("0")}).fill(0),'
# datajs_str = re.sub(r"\"y\":\[(0\,,?)*", replace_zeros, datajs_str)

with open("assets/data.js", "w") as f:
    f.write(f"months={json.dumps(months)}\n{datajs_str}")
