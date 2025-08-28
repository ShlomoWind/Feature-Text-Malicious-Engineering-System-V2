import json
import re
from datetime import datetime
from Preprocessor.src.processor import Preprocessor
from nltk.sentiment import SentimentIntensityAnalyzer


class DataEnricher:
    def __init__(self,message):
        if isinstance(message, str):
            self.message_dict = json.loads(message)
        else:
            self.message_dict = message

        with open("weapon_list.txt", "r") as f:
            self.weapons = [line.strip() for line in f.readlines()]

# Activates all functions on the message
    def enriched(self):
        clean_text = self.message_dict.get("clean_text", "")
        self.message_dict['sentiment'] = self.sentiment_type(clean_text)
        if self.weapon_blacklist(clean_text):
            self.message_dict['weapons_detected'] = self.weapon_blacklist(clean_text)
        if self.extract_latest_timestamp(clean_text):
            self.message_dict['relevant_timestamp'] = self.extract_latest_timestamp(clean_text)
        return self.message_dict

# Finding the sentiment of the text - positive, negative, or neutral
    def sentiment_type(self,text):
        score = SentimentIntensityAnalyzer().polarity_scores(text)
        if score['compound'] >= 0.5:
            return 'positive'
        elif score['compound'] <= -0.5:
            return 'negative'
        else:
            return 'neutral'

# Finding names of weapons according to a blacklist
    def weapon_blacklist(self, text):
        processed_weapons = []
        for w in self.weapons:
            processed_dict = Preprocessor(w).process()
            processed_weapons.append(processed_dict.get("clean_text", w))
        text_clean = text.lower()
        found_weapons = [weapon for weapon in processed_weapons if weapon in text_clean]
        return found_weapons if found_weapons else None


    def extract_latest_timestamp(self, text):
        matches = re.findall(r"\d{4}-\d{2}-\d{2}", text)
        if not matches:
            return None
        dates = [datetime.strptime(m, "%Y-%m-%d") for m in matches]
        latest_date = max(dates)
        return latest_date.strftime("%Y-%m-%d")
