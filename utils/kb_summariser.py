#import config
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
import pandas as pd
import logging
import os

#model = 'facebook/bart-large-cnn'
#model = 'facebook/mbart-large-cc25'
#model = 'cointegrated/rut5-base-absum'
#model = 'utrobinmv/t5_summary_en_ru_zh_base_2048'
#model = 'YorkieOH10/Meta-Llama-3.1-8B-Instruct-Q8_0-GGUF'
#model = 'google/mt5-base'
#model = 'ai-forever/FRED-T5-1.7B'
#model = 'ai-forever/ruGPT-3.5-13B'


os.environ['CURL_CA_BUNDLE'] = '' 
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['HF_HUB_DISABLE_SSL'] = 'true'
os.environ['TRANSFORMERS_OFFLINE'] = '1'

model = 'RussianNLP/FRED-T5-Summarizer'
#local_model = '../models/FRED-T5-Summarizer'
local_model = model
tokenizer = AutoTokenizer.from_pretrained(local_model)
llm = AutoModelForSeq2SeqLM.from_pretrained(local_model)

#summarizer = pipeline("summarization", model=model)
summarizer = pipeline("summarization", model=llm, tokenizer=tokenizer)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('summarization')

processed = 0

def summarise(text, max_length=1024, min_length=256, do_sample=False):
    tokenizer = AutoTokenizer.from_pretrained(local_model)
    length = len(tokenizer.encode(text))
    global processed
    if length < max_length:
        result = text
    elif summary := summarizer(
        text,
        max_length=length // 2,
        min_length=min_length,
        do_sample=do_sample,
    ):
        result = summary[0]['summary_text'].strip()
    else:
        result = text[:max_length]
    logger.info(f'{processed}: {text[:64]}({len(text)}:{length})==>{result[:64]}({len(result)})')
    processed = processed + 1
    return result

if __name__ == "__main__":
    df = pd.read_csv('./output/articles_data.csv', encoding="utf-8")
    df = df[df['refs'].notnull() & (df['refs'] != '')]
    df['solution'] = df['refs'].apply(lambda x: summarise(x, max_length=256, min_length=64, do_sample=False))
    df.to_csv('./output/articles_data_summ.csv', index=False)

