import pandas as pd
import re
import os 



swedish_stop_words = {'varför', 'än', 'dig', 'våra', 'om', 'nu', 'vad', 'icke', 'ju', 'hennes', 'min', 'vårt', 'bli', 'hade', 'vilket', 'och', 'vart', 'sig', 'ha', 'oss', 'sedan', 'blir', 'sådan', 'dina', 'denna', 'på', 'mellan', 'ett', 'av', 'samma', 'ut', 'dess', 'men', 'han', 'allt', 'för', 'varit', 'din', 'ni', 'jag', 'med', 'i', 'detta', 'att', 'mycket', 'själv', 'varje', 'skulle', 'till', 'de', 'er', 'något', 'den', 'henne', 'över', 'hon', 'vår', 'sitta', 'hans', 'någon', 'vara', 'vem', 'deras', 'era', 'det', 'sådana', 'när', 'sin', 'blivit', 'mot', 'kunde', 'mig', 'så', 'då', 'en', 'var', 'kan', 'vi', 'vilka', 'inom', 'du', 'sådant', 'honom', 'är', 'alla', 'vilkas', 'från', 'dem', 'här', 'utan', 'åt', 'vilken', 'vid', 'upp', 'hur', 'ej', 'ingen', 'blev', 'sina', 'mina', 'under', 'ditt', 'eller', 'där', 'vars', 'dessa', 'efter', 'ert', 'några', 'man', 'har', 'inte', 'som', 'mitt'}
english_stop_words = {'ma', 'of', 'did', 'itself', 'there', "it's", "he'd", 'with', "needn't", 'aren', 'his', 'himself', 'as', 'more', "i've", 'such', "should've", 'on', 'her', "you'll", 'in', 'here', 'my', 'when', 'again', 'ain', 'you', "we've", 'out', 'by', 'if', 'from', "won't", 'y', 'its', 'them', "mightn't", 'against', 'before', 'for', "it'd", 'over', 'now', 'yourself', 've', 'their', 'having', 'were', 'where', 'that', 'me', 'hadn', 'below', 'hers', 'themselves', 'has', 'then', 'whom', 'be', 'after', 'don', "they'll", 'they', 'are', 'about', 'does', 'to', 'yours', 'our', 'll', 'isn', 's', 'and', 'ourselves', 'into', 'than', 'your', 'he', 'i', 're', "we'd", 'at', 'she', 'do', 'some', "didn't", 'have', 'is', 'but', 'between', 'the', 'wouldn', 'will', "we'll", 'through', "she'd", "hadn't", "they've", 'doesn', "he's", 'nor', 'just', 'mightn', 'hasn', "you've", 'up', "wasn't", 'most', 'not', 'ours', 'until', "shan't", 'yourselves', 'shan', "she'll", "wouldn't", 'other', 'doing', 'down', "shouldn't", 'few', "couldn't", "you'd", 'so', 'couldn', 'can', 'had', 'own', 'wasn', 'once', 'weren', 'd', 'each', "hasn't", 'those', 'above', "i'd", 'theirs', "doesn't", 'mustn', 'these', 'should', "don't", "she's", "you're", 'was', 'who', "aren't", 'being', 'or', 'all', 'while', 'herself', "that'll", 'both', 'any', 'been', 'haven', 't', "i'm", 'needn', "they're", 'this', "mustn't", 'm', "it'll", 'how', 'why', 'o', 'an', 'won', 'a', "we're", 'myself', 'am', 'didn', "i'll", 'under', 'off', 'too', 'it', 'very', 'which', 'only', "haven't", 'same', "he'll", 'no', 'further', 'because', 'shouldn', 'during', 'him', "they'd", 'we', 'what', "weren't", "isn't"}
custom_stop_words = set(["vissa", "andra", "ord", "som", "inte", "finns", "i", "nltk"])  


stop_words = swedish_stop_words.union(english_stop_words, custom_stop_words)

df = pd.read_excel("exceldatafrom.xlsx")  

print(df["description"])


def remove_stop_words(text):
    if isinstance(text, str):  
        text = text.lower()  
        word = re.findall(r'\b\w+\b', text) 
        filtered = [w for w in word if w not in stop_words]  
        return " ".join(filtered)  
    return ""  

df['description_filtered'] = df['description'].apply(remove_stop_words)

# print(df)
print(df["description_filtered"])


