import pandas as pd
df = pd.read_csv('data.csv')
df.pop('Unnamed: 0')
from deep_translator import GoogleTranslator

from googletrans import Translator

translator = GoogleTranslator(source='ja', target='en')
# Function to batch translate a column
def batch_translate(column):
    list_trans = list()
    for text in column:
        trans_text = translator.translate(text)
        print(trans_text)
        list_trans.append(trans_text)
    # return [translator.translate(text) for text in column]
    return list_trans

# Apply batch translation to each column
for col in df.columns:
    df[col] = batch_translate(df[col])
print()