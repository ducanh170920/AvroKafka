import json
import re
from pyvi import ViTokenizer, ViPosTagger
def nomalize(data):

    for j in range(len(data)):
        data[j] = data[j].replace("_", " ").lower()
    result = []
    #filter
    for word in data:
        if(re.search("^\w+\s", word)):
            result.append(word)
    data = list(dict.fromkeys(data))
    return result;

lines = open("C:\\Users\\hoang\\OneDrive\\Desktop\\data\\data_1.json","r",encoding="utf8")
f    = open("C:\\Users\\hoang\\OneDrive\\Desktop\\data\\data_anhhd25_1.json","a",encoding="utf8")
i = 0

for line in lines:
  
    i = i + 1
    if i == 1:
       f.write(line)
    if i == 2:
        i = 0
        title = json.loads(line)['title']
        data = ViTokenizer.tokenize(title).split(" ")
        data = nomalize(data)
        print(data)
        patterm = "{" + "\"suggest_title\": [\"" + '", "'.join(data) + "\"]}\n"
        f.write(patterm)


f.close()
lines.close()





