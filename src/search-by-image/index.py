import os
import shutil
import logging
import datetime
import aiohttp
import asyncio
from pathlib import Path

import cv2
from fastapi import FastAPI, File, UploadFile
from DeepImageSearch import Load_Data, Search_Setup
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import uvicorn


os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
load_dotenv()


IMAGE_DIR = "data"
UPLOAD_FOLDER = "uploads"
Path(IMAGE_DIR).mkdir(parents=True, exist_ok=True)
Path(UPLOAD_FOLDER).mkdir(parents=True, exist_ok=True)

# MongoDB
MONGO_URL = os.getenv("MONGODB")
client = AsyncIOMotorClient(MONGO_URL)
db = client["homeMadeFood"]
meals_collection = db["meals"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB
def get_image_filename(meal_id):
    return os.path.join(IMAGE_DIR, f"meal_{meal_id}.jpg")

async def fetch_image_urls():
    query = {"updatedAt": {"$gt": datetime.datetime.utcnow() - datetime.timedelta(days=365)}}
    projection = {"_id": 1, "images": 1}
    urls = []

    async for doc in meals_collection.find(query, projection):
        for img in doc.get("images", []):
            url = img.get("secure_url")
            if url:
                urls.append((str(doc["_id"]), url))
    return urls

async def download_image(session, url, filename):
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(filename, 'wb') as f:
                    f.write(await resp.read())
                return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
    return False

async def download_images(image_data):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for meal_id, url in image_data:
            filename = get_image_filename(meal_id)
            if not os.path.exists(filename):
                tasks.append(download_image(session, url, filename))
        await asyncio.gather(*tasks)

async def update_index_with_new_images():
    image_data = await fetch_image_urls()  
    await download_images(image_data)  
    image_list = Load_Data().from_folder([IMAGE_DIR])  
    search_engine.run_index()  

# FastAPI
app = FastAPI()
search_engine = None

@app.on_event("startup")
async def startup_event():
    global search_engine
    if os.path.exists('.deep_image_search'):
        shutil.rmtree('.deep_image_search')


    image_data = await fetch_image_urls()
    await download_images(image_data)
    
    image_list = Load_Data().from_folder([IMAGE_DIR])
    logger.info(f"Total images indexed: {len(image_list)}")

    search_engine = Search_Setup(image_list=image_list)
    search_engine.run_index()

    asyncio.create_task(update_index_with_new_images())

@app.get("/")
def home():
    return {"message": "FastAPI is running with image search!"}

@app.post("/search-by-image/")
async def search_by_image(file: UploadFile = File(...), top_n: int = 5):
    file_location = f"{UPLOAD_FOLDER}/{file.filename}"
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    search_results = search_engine.get_similar_images(image_path=file_location, number_of_images=top_n)
    meal_ids = []
    for path in search_results.values():
        filename = os.path.basename(path)
        if filename.startswith("meal_") and filename.endswith(".jpg"):
            meal_id = filename.replace("meal_", "").replace(".jpg", "")
            meal_ids.append(meal_id) 

    return {
        "query_image": file_location,
        "similar_meal_ids": meal_ids
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

