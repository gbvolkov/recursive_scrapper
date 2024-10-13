import logging
import os
import pandas as pd
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM

# Set environment variables to handle SSL issues and enable offline mode if necessary
os.environ['CURL_CA_BUNDLE'] = '' 
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['HF_HUB_DISABLE_SSL'] = 'true'
os.environ['TRANSFORMERS_OFFLINE'] = '1'

# Model configuration
model_name = 'RussianNLP/FRED-T5-Summarizer'
local_model = model_name  # Change to your local model path if needed

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('summarization')

# Check if GPU is available and set the device accordingly
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

# Load the tokenizer and model
tokenizer = AutoTokenizer.from_pretrained(local_model)
model = AutoModelForSeq2SeqLM.from_pretrained(local_model).to(device)

# Initialize summarizer pipeline with device
summarizer = pipeline("summarization", model=model, tokenizer=tokenizer, device=0 if torch.cuda.is_available() else -1)

processed = 0

def summarise(text, max_length=256, min_length=64, do_sample=False):
    global processed
    try:
        # Tokenize the input text to get the token count
        inputs = tokenizer.encode_plus(
            text,
            return_tensors='pt',
            truncation=False,
        )
        input_ids = inputs['input_ids'].to(device)  # Move input to the correct device
        length = input_ids.shape[1]
        model_max_length = tokenizer.model_max_length

        # Adjust model_max_length if necessary
        if model_max_length > 1024:
            model_max_length = 512  # Set to the actual model's maximum input length

        if length <= model_max_length:
            # Input is within acceptable length; proceed to summarize
            summary = summarizer(
                text,
                max_length=min(max_length, length),
                min_length=min(min_length, length // 4),
                do_sample=do_sample,
                truncation=True,
            )[0]['summary_text'].strip()
            result = summary
        else:
            # Input exceeds maximum length; need to split into chunks
            chunk_size = model_max_length - 2  # Account for special tokens
            input_ids_list = input_ids[0]  # Get the tensor of input IDs

            # Split input_ids into chunks
            num_chunks = (length + chunk_size - 1) // chunk_size  # Ceiling division
            input_id_chunks = torch.split(input_ids_list, chunk_size)

            summaries = []
            for chunk in input_id_chunks:
                chunk_text = tokenizer.decode(chunk, skip_special_tokens=True)
                # Summarize each chunk
                summary = summarizer(
                    chunk_text,
                    max_length=max_length,
                    min_length=min_length,
                    do_sample=do_sample,
                    truncation=True,
                )[0]['summary_text'].strip()
                summaries.append(summary)

            # Combine summaries of chunks
            combined_summary = ' '.join(summaries)

            # Tokenize the combined summary to check its token length
            combined_summary_tokens = tokenizer.encode(combined_summary, truncation=False)
            combined_length = len(combined_summary_tokens)

            if combined_length > max_length:
                # Summarize the combined summary to reduce token length
                final_summary = summarizer(
                    combined_summary,
                    max_length=max_length,
                    min_length=min_length,
                    do_sample=do_sample,
                    truncation=True,
                )[0]['summary_text'].strip()
                result = final_summary
            else:
                result = combined_summary

            # Ensure the final summary does not exceed max_length tokens
            result_tokens = tokenizer.encode(result, truncation=True, max_length=max_length)
            result = tokenizer.decode(result_tokens, skip_special_tokens=True)
    except Exception as e:
        logger.error(f"Error during summarization: {e}")
        # Fallback: truncate the input tokens to max_length
        result_tokens = input_ids[0][:max_length]
        result = tokenizer.decode(result_tokens, skip_special_tokens=True)

    output_length = len(tokenizer.encode(result, truncation=False))
    logger.info(f'{processed}: Input length: {length} tokens ==> Output length: {output_length} tokens')
    processed += 1
    return result

if __name__ == "__main__":
    df = pd.read_csv('./output/articles_data.csv', encoding="utf-8")
    # Filter out rows with null or empty 'refs' column
    df = df[df['refs'].notnull() & (df['refs'] != '')]
    # Apply the summarise function to the 'refs' column
    df['solution'] = df['refs'].apply(lambda x: summarise(x, max_length=256, min_length=64, do_sample=False))
    # Save the results to a new CSV file
    df.to_csv('./output/articles_data_summ.csv', index=False)
