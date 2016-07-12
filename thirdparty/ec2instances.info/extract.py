import json
from urllib2 import urlopen

url = "https://raw.githubusercontent.com/powdahound/ec2instances.info/master/www/instances.json"
data = {}
for inst in json.load(urlopen(url)):
    data[inst["instance_type"]] = inst["memory"]

json.dump(data, open("memory.json", "w"), indent=2)

