from fastapi import FastAPI

app = FastAPI()

# MongoDB connection
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "my_ready_data")

client = MongoClient(MONGO_URL)
db = client[DB_NAME]

@app.route('/antisemitic', methods=['GET'])
def get_antisemitic_tweets():
    """Endpoint to retrieve all antisemitic tweets"""
    try:
        collection = db["tweets_antisemitic"]
        tweets = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB _id
        return jsonify({
            "status": "success",
            "count": len(tweets),
            "data": tweets
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving antisemitic tweets: {str(e)}"
        }), 500

@app.route('/not_antisemitic', methods=['GET'])
def get_not_antisemitic_tweets():
    """Endpoint to retrieve all non-antisemitic tweets"""
    try:
        collection = db["tweets_not_antisemitic"]
        tweets = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB _id
        return jsonify({
            "status": "success",
            "count": len(tweets),
            "data": tweets
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error retrieving non-antisemitic tweets: {str(e)}"
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "data-retrieval"
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

# data_retrieval/requirements.txt
Flask==2.3.3
pymongo==4.5.0

# data_retrieval/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

EXPOSE 5000

CMD ["python", "app.py"]