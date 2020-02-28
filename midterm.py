from pyspark import SparkConf, SparkContext
import math
from nltk.corpus import stopwords

conf = SparkConf().setMaster("local").setAppName("Stats")
sc = SparkContext(conf= conf)

text = sc.textFile("Encrypted-1.txt")

def output(word):
    print(word)

def isalpha(val):
    ret = " "
    if val.isalnum():
        ret = val
    return ret


def decrypt(offset,line):
    new_line = ""
    for char in line:
        if char.isalnum():
            asci = ord(char)
            totl = asci+offset
            if totl > 90 and asci <=90 and asci >=65:
                totl -= 26
            if totl > 122 and asci <= 122 and asci >=97:
                totl -= 26
            new_line += chr(totl)
        else:
            new_line+=char
    return new_line

charwords =text.flatMap(lambda line: list(line)) 

chartotal = charwords.count()

char_distribution = text.flatMap(lambda line: list(line)) \
             .map(lambda word: (isalpha(word.lower()), word)) \
             .reduceByKey(lambda a, b: a +" " + b)\
             .map(lambda word: (word[0] ,len(word[1].split(" ")), word[1]))\
             .map(lambda word: (word[0],word[1]/float(chartotal)*100)) \
             .sortBy(lambda a: a[1],ascending=False)
            # .filter(lambda line : line != text.first)

char_distribution.foreach(output) 

expected = sc.textFile("frequency.txt")
expected_distribution = expected.map(lambda line: (line.split(" ")[0],float(line.split(" ")[1] ))) \
            .sortBy(lambda a: a[1],ascending=False)

expected_distribution.foreach(output) 

offset = ord(char_distribution.take(2)[1][0]) - ord(expected_distribution.take(1)[0][0])

print(offset)

new_chars = text.map(lambda line: decrypt(offset,line)).saveAsTextFile("output")
#new_chars.foreach(output)


