import os
import streamlit as st
from kafka import KafkaProducer
from googleapiclient.discovery import build
from textblob import TextBlob
from transformers import BertTokenizer, BertForSequenceClassification
import torch
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
import threading
import time
import re
import matplotlib.pyplot as plt
from collections import defaultdict
from wordcloud import WordCloud
from kafka.errors import KafkaError
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# YouTube Data API
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# Sentiment counts
sentiment_counts = defaultdict(int)
comments_processed = 0
all_comments_text = []

# Load BERT model
tokenizer = BertTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')
model = BertForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')

# Azure Setup
azure_key = os.getenv("AZURE_KEY")
azure_endpoint = os.getenv("AZURE_ENDPOINT")

def authenticate_client():
    ta_credential = AzureKeyCredential(azure_key)
    return TextAnalyticsClient(endpoint=azure_endpoint, credential=ta_credential)

client = authenticate_client()

# Kafka Producer (Aiven)
producer = None

def create_kafka_producer():
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol='SSL',
            ssl_cafile=os.getenv("AIVEN_CA_PATH"),
            ssl_certfile=os.getenv("AIVEN_CERT_PATH"),
            ssl_keyfile=os.getenv("AIVEN_KEY_PATH")
        )
    except KafkaError as e:
        st.error(f"Kafka producer creation failed: {e}")

# Sentiment Analysis
def analyze_sentiment_textblob(comment):
    blob = TextBlob(comment)
    return blob.sentiment.polarity

def analyze_sentiment_bert(comment):
    inputs = tokenizer(comment, return_tensors='pt', truncation=True, padding=True)
    outputs = model(**inputs)
    logits = outputs.logits
    sentiment_score = torch.softmax(logits, dim=1).tolist()[0]
    return sentiment_score

def analyze_sentiment_azure(comment):
    try:
        response = client.analyze_sentiment(documents=[comment])[0]
        return response.sentiment.capitalize()
    except Exception as e:
        print(f"Azure API Error: {e}")
        return 'Neutral'

def categorize_sentiment_textblob(score):
    if score > 0.1:
        return 'Positive'
    elif score < -0.1:
        return 'Negative'
    else:
        return 'Neutral'

def categorize_sentiment_bert(score):
    positive, neutral, negative = score[4], score[2], score[0]
    if positive > max(neutral, negative):
        return 'Positive'
    elif negative > max(positive, neutral):
        return 'Negative'
    else:
        return 'Neutral'

def save_sentiment_to_db(sentiment_category):
    sentiment_counts[sentiment_category] += 1

def generate_pie_chart(sentiment_counts, chart_container):
    labels = list(sentiment_counts.keys())
    sizes = list(sentiment_counts.values())

    if sum(sizes) == 0:
        chart_container.write("No sentiment data available.")
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    chart_container.pyplot(fig)

def generate_word_cloud(all_comments_text, cloud_container):
    if not all_comments_text:
        cloud_container.write("No comments data available.")
        return

    text = ' '.join(all_comments_text)
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    cloud_container.pyplot(fig)

def fetch_video_comments_and_analyze(video_id, analysis_method):
    global comments_processed, all_comments_text
    request = youtube.commentThreads().list(
        videoId=video_id,
        part='snippet',
        maxResults=100
    )

    while request is not None:
        try:
            response = request.execute()
            for item in response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                all_comments_text.append(comment)
                comments_processed += 1

                sentiment_score, sentiment_category = analyze_comment(comment, analysis_method)

                if producer is None:
                    create_kafka_producer()

                producer.send('youtube-video-comments', value=comment.encode('utf-8'))
                producer.flush()

                save_sentiment_to_db(sentiment_category)

            request = youtube.commentThreads().list_next(request, response)

        except HttpError as e:
            if e.resp.status == 403 and 'commentsDisabled' in str(e):
                st.error("Comments are disabled for this video.")
            else:
                st.error(f"Error fetching comments: {e}")
            break

def get_live_chat_id(video_id):
    try:
        response = youtube.videos().list(
            part='liveStreamingDetails',
            id=video_id
        ).execute()

        if 'items' in response and len(response['items']) > 0:
            live_stream_details = response['items'][0].get('liveStreamingDetails', None)
            if live_stream_details and 'activeLiveChatId' in live_stream_details:
                return live_stream_details['activeLiveChatId']
    except Exception as e:
        st.error(f"Error fetching live chat ID: {e}")
        return None

def fetch_live_chat_comments_and_analyze(live_chat_id, analysis_method):
    global comments_processed, all_comments_text
    request = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part='snippet,authorDetails',
        maxResults=200
    )

    while True:
        try:
            response = request.execute()
            for item in response.get('items', []):
                if 'displayMessage' in item['snippet']:
                    comment = item['snippet']['displayMessage']
                    all_comments_text.append(comment)
                    comments_processed += 1

                    sentiment_score, sentiment_category = analyze_comment(comment, analysis_method)

                    if producer is None:
                        create_kafka_producer()

                    producer.send('youtube-live-comments', value=comment.encode('utf-8'))
                    producer.flush()

                    save_sentiment_to_db(sentiment_category)

            time.sleep(5)

        except HttpError as e:
            if e.resp.status == 403:
                st.error("Live chat is disabled for this stream.")
            else:
                st.error(f"An error occurred: {e}")
            break
        except Exception as e:
            print(f"Error fetching live chat comments: {e}")
            time.sleep(5)

def analyze_comment(comment, analysis_method):
    sentiment_score = None
    sentiment_category = None

    if analysis_method == "TextBlob":
        sentiment_score = analyze_sentiment_textblob(comment)
        sentiment_category = categorize_sentiment_textblob(sentiment_score)
    elif analysis_method == "BERT":
        sentiment_score = analyze_sentiment_bert(comment)
        sentiment_category = categorize_sentiment_bert(sentiment_score)
    elif analysis_method == "Azure":
        sentiment_category = analyze_sentiment_azure(comment)

    return sentiment_score, sentiment_category

def extract_video_id(url):
    match = re.search(r'v=([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

def start_streamlit_dashboard():
    st.title("YouTube Video/Live Stream Sentiment Analysis with Aiven Kafka")

    video_url = st.text_input("Enter YouTube Video/Live Stream URL:", "")
    analysis_method = st.selectbox("Choose Sentiment Analysis Method", ["TextBlob", "BERT", "Azure"])

    chart_container = st.empty()
    cloud_container = st.empty()
    count_container = st.empty()

    if video_url:
        video_id = extract_video_id(video_url)
        if video_id:
            create_kafka_producer()
            live_chat_id = get_live_chat_id(video_id)
            if live_chat_id:
                st.write(f"Analyzing live chat comments for live stream ID: {video_id} using {analysis_method}...")
                threading.Thread(target=fetch_live_chat_comments_and_analyze, args=(live_chat_id, analysis_method), daemon=True).start()
            else:
                st.write(f"Analyzing video comments for ID: {video_id} using {analysis_method}...")
                threading.Thread(target=fetch_video_comments_and_analyze, args=(video_id, analysis_method), daemon=True).start()

            while True:
                count_container.write(f"Comments Processed: {comments_processed}")
                generate_pie_chart(sentiment_counts, chart_container)
                generate_word_cloud(all_comments_text, cloud_container)
                time.sleep(5)
        else:
            st.error("Invalid YouTube URL.")

if __name__ == "__main__":
    start_streamlit_dashboard()
