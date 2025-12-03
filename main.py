import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from pydantic import BaseModel
from bson import ObjectId

app = FastAPI()

# MongoDB Config
# Use MONGODB_URL env var if available, otherwise default to localhost
MONGO_URL = os.environ.get("MONGODB_URL", "mongodb+srv://wzj0311:q0Qy4XZAjc8RfDCd@emogo-backend.dd1gyz6.mongodb.net/")
client = AsyncIOMotorClient(MONGO_URL)
db = client.emogo_db
fs = AsyncIOMotorGridFSBucket(db)

templates = Jinja2Templates(directory="templates")

# Models
class SentimentModel(BaseModel):
    timestamp: str
    data: Dict[str, Any]

class GPSModel(BaseModel):
    timestamp: str
    latitude: float
    longitude: float

@app.get("/")
async def root():
    return {"message": "EmoGo Backend is running"}

@app.post("/upload/sentiment")
async def upload_sentiment(sentiment: SentimentModel):
    await db.sentiments.insert_one(sentiment.dict())
    return {"status": "success"}

@app.post("/upload/gps")
async def upload_gps(gps: GPSModel):
    await db.gps.insert_one(gps.dict())
    return {"status": "success"}

@app.post("/upload/vlog")
async def upload_vlog(file: UploadFile = File(...)):
    # Store file in GridFS
    grid_in = fs.open_upload_stream(
        file.filename, metadata={"contentType": file.content_type}
    )
    await grid_in.write(await file.read())
    await grid_in.close()
    return {"status": "success", "file_id": str(grid_in._id)}

@app.get("/data", response_class=HTMLResponse)
async def view_data(request: Request):
    # Fetch recent data
    sentiments = await db.sentiments.find().sort("_id", -1).to_list(100)
    gps_data = await db.gps.find().sort("_id", -1).to_list(100)
    
    vlogs = []
    # GridFS find returns a cursor
    cursor = fs.find().sort("uploadDate", -1).limit(100)
    async for grid_out in cursor:
        vlogs.append({"filename": grid_out.filename, "id": str(grid_out._id)})
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "sentiments": sentiments,
        "gps_data": gps_data,
        "vlogs": vlogs
    })

@app.get("/download/vlog/{file_id}")
async def download_vlog(file_id: str):
    try:
        oid = ObjectId(file_id)
        grid_out = await fs.open_download_stream(oid)
        
        async def file_iterator():
            while True:
                chunk = await grid_out.read(1024 * 1024)  # Read in 1MB chunks
                if not chunk:
                    break
                yield chunk

        return StreamingResponse(
            file_iterator(), 
            media_type=grid_out.metadata.get("contentType", "application/octet-stream"),
            headers={"Content-Disposition": f"attachment; filename={grid_out.filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")
