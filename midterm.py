from pyspark import SparkConf, SparkContext
import math
from nltk.corpus import words
import enchant

conf = SparkConf().setMaster("local").setAppName("Stats")

sc = SparkContext(conf= conf)

filename = "Encrypted-1.txt"

text = sc.textFile(filename)

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
            new_line+=" "
    return new_line

def is_word(word):
    #dic = enchant.Dict("en_US")
    #return dic.check(word)
    
    state = word.lower() in words.words()
    return str2bool(word, state)
    #    return (word,1,"english")
    #return (word,0," Not english")

def str2bool(word,v):
    if v == True:
        return (word, 1, "English")
    else:
        return (word, 0, "Not english")

def word_count(word):
    print("there are ", word ,"english words in the text")

charwords =text.flatMap(lambda line: list(line)) 

chartotal = charwords.count()

char_distribution = text.flatMap(lambda line: list(line)) \
             .map(lambda word: (isalpha(word.lower()), word)) \
             .reduceByKey(lambda a, b: a +" " + b)\
             .map(lambda word: (word[0] ,len(word[1].split(" ")), word[1]))\
             .map(lambda word: (word[0],word[1]/float(chartotal)*100)) \
             .sortBy(lambda a: a[1],ascending=False)

#char_distribution.foreach(output) 

expected = sc.textFile("frequency.txt")

expected_distribution = expected.map(lambda line: (line.split(" ")[0],float(line.split(" ")[1] ))) \
            .sortBy(lambda a: a[1],ascending=False)

#expected_distribution.foreach(output) 

offset = ord(char_distribution.take(2)[1][0]) - ord(expected_distribution.take(1)[0][0])

print(offset)
print(is_word("word"))

new_chars = text.map(lambda line: decrypt(offset,line))

save = new_chars.saveAsTextFile("decrypted-"+filename)
#.map(lambda word : (word, word))

eng = new_chars.flatMap(lambda line : line.split(" ")) \
    .distinct() \
    .map(lambda word: is_word(word)) \
    .foreach(output)
