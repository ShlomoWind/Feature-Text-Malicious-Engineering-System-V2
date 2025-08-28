from nltk.corpus import stopwords
import json
import string
import nltk
import spacy
import re

nlp = spacy.load("en_core_web_sm")
nltk.download('stopwords')
STOP_WORDS = set(stopwords.words('english'))

class Preprocessor:
    def __init__(self,message):
        if isinstance(message, str):
            self.message_dict = json.loads(message)
        else:
            self.message_dict = message

    def process(self):
        original_text = self.message_dict.get("text", "")
        cleaned = original_text.translate(str.maketrans('', '', string.punctuation))
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.lower()
        cleaned = ' '.join([word for word in cleaned.split() if word not in STOP_WORDS])
        msg = nlp(cleaned)
        cleaned = ' '.join([token.lemma_ for token in msg])
        self.message_dict['clean_text'] = cleaned
        return self.message_dict