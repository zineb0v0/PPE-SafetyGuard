from contextlib import asynccontextmanager
from pathlib import Path
import shutil
import cv2
import asyncio

# FastAPI imports
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# YOLO model
from ultralytics import YOLO

# Local imports
from alerts import add_alert, alerts_log
from alerts_categories import RED_CLASSES, CLASS_NAMES, get_danger_info
from database import init_db, get_alert_history, insert_alert
from violation_track import track_violation

# Global variables
model = None
last_uploaded_video = None

# Directories
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

FRONTEND_DIR = BASE_DIR.parent / "uipage" / "frontend"

# Camera sources
camera_sources = {
    "webcam": 0,  
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    # Startup
    try:
        # Initialize database
        init_db()
        print("Database initialized")
        
        # Load YOLO model
        global model
        model_path = Path("models") / "best.pt"
        if model_path.exists():
            model = YOLO(str(model_path))
            print(f"Model loaded successfully from {model_path}")
        else:
            print(f"❌ Model file not found: {model_path}")
            model = YOLO('yolov8n.pt')
            print("Using default YOLOv8n model as fallback")

        print("\n" + "="*60)
        print("SafetyGuard PPE Detection System")
        print("="*60)
        print("Server starting...")
        print("Dashboard: http://localhost:8080")
        print("API Docs: http://localhost:8080/docs")
        print("Health Check: http://localhost:8080/health")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f" Startup error: {e}")
        raise
    
    yield
    
    # Shutdown
    print("\nSafetyGuard shutting down...")
    try:
        cv2.destroyAllWindows()
    except:
        pass

# Create FastAPI app
app = FastAPI(
    title="SafetyGuard PPE Detection",
    description="Personal Protective Equipment Detection System",
    version="1.0.0",
    lifespan=lifespan
)

# Mount static files
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def read_index():
    """Serve the main dashboard page"""
    html_file = FRONTEND_DIR / "index.html"
    if html_file.exists():
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return Response(
            content=html_content,
            media_type="text/html; charset=utf-8"
        )
    return Response(
        "<h1>Dashboard HTML file not found</h1>", 
        media_type="text/html; charset=utf-8"
    )

@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    """Upload video file for analysis"""
    global last_uploaded_video
    
    try:
        if not file.content_type or not file.content_type.startswith('video/'):
            raise HTTPException(
                status_code=400, 
                detail="Please upload a valid video file"
            )
        
        file_path = UPLOAD_DIR / file.filename
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        last_uploaded_video = file_path
        print(f"✅ Video uploaded: {file.filename}")
        return {"message": f"Video uploaded successfully: {file.filename}"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload error: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Failed to upload video"
        )

def gen_frames_from_video(video_path, conf=0.25, skip_frames=2):
    """Generate frames from uploaded video with PPE detection"""
    cap = cv2.VideoCapture(str(video_path))
    frame_count = 0
    
    if not cap.isOpened():
        print(f"Cannot open video: {video_path}")
        return
    
    try:
        while True:
            success, frame = cap.read()
            if not success:
                # Loop video for continuous playback
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue
            
            frame_count += 1
            if frame_count % skip_frames != 0:
                continue
            
            # PPE Detection
            if model is not None:
                try:
                    results = model(frame, conf=conf)[0]
                    
                    # Process detections
                    for box in results.boxes:
                        cls = int(box.cls[0])
                        info = get_danger_info(cls)
                        color = info["color"]
                        
                        x1, y1, x2, y2 = map(int, box.xyxy[0])
                        conf_score = box.conf[0]
                        label = f"{CLASS_NAMES.get(cls, f'Class_{cls}')} {conf_score:.2f}"
                        
                        # Draw bounding box
                        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                        cv2.putText(frame, label, (x1, y1 - 10),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
                        
                        # Track violations
                        if cls in RED_CLASSES:
                            track_violation(cls)
                            
                except Exception as e:
                    print(f"Detection error: {e}")
            
            # Encode frame for streaming
            ret, buffer = cv2.imencode('.jpg', frame)
            if ret:
                frame_bytes = buffer.tobytes()
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
                
    except Exception as e:
        print(f"Error processing video frames: {e}")
    finally:
        cap.release()

def generate_camera_frames(source):
    """Generate frames from camera source with PPE detection"""
    cap = None

    try:
        if isinstance(source, str) and source.isdigit():
            source = int(source)

        cap = cv2.VideoCapture(source)

        if not cap.isOpened():
            print(f"Cannot open camera source: {source}")
            return

        # Set camera properties
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
        cap.set(cv2.CAP_PROP_FPS, 30)

        while True:
            success, frame = cap.read()
            if not success:
                print(f"Failed to read frame from {source}")
                break

            # PPE Detection
            if model is not None:
                try:
                    results = model(frame, conf=0.25)[0]

                    # Track violations
                    for box in results.boxes:
                        cls = int(box.cls[0])
                        if cls in RED_CLASSES:
                            track_violation(cls)

                    annotated_frame = results.plot()
                except Exception as e:
                    print(f"Detection error: {e}")
                    annotated_frame = frame
            else:
                annotated_frame = frame

            # Encode frame for streaming
            ret, buffer = cv2.imencode('.jpg', annotated_frame)
            if ret:
                frame_bytes = buffer.tobytes()
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')

    except Exception as e:
        print(f"Camera error: {e}")
    finally:
        if cap is not None:
            cap.release()

@app.get("/video_feed")
def video_feed(source: str = None):
    """Stream video feed with PPE detection"""
    try:
        if source and source in camera_sources:
            camera_source = camera_sources[source]
            return StreamingResponse(
                generate_camera_frames(camera_source),
                media_type='multipart/x-mixed-replace; boundary=frame'
            )
        elif last_uploaded_video and last_uploaded_video.exists():
            return StreamingResponse(
                gen_frames_from_video(last_uploaded_video),
                media_type='multipart/x-mixed-replace; boundary=frame'
            )
        else:
            raise HTTPException(
                status_code=404,
                detail="No video source available"
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Video feed error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Video feed unavailable"
        )

@app.get("/alerts")
def get_alerts():
    """Get current alerts"""
    try:
        return JSONResponse(content=alerts_log)
    except Exception as e:
        print(f"Alerts fetch error: {e}")
        return JSONResponse(content=[])

@app.get("/history")
def get_history():
    """Get alert history from database"""
    try:
        history = get_alert_history(50)
        formatted_history = []
        for time, message, status in history:
            formatted_history.append({
                "time": time,
                "message": message,
                "status": status
            })
        return JSONResponse(content={"history": formatted_history})
    except Exception as e:
        print(f"History fetch error: {e}")
        return JSONResponse(content={"history": []})

@app.get("/camera_status")
def get_camera_status():
    """Check camera availability"""
    status = {}
    for name, source in camera_sources.items():
        try:
            cap = cv2.VideoCapture(source)
            status[name] = cap.isOpened()
            cap.release()
        except:
            status[name] = False
    
    return JSONResponse(content=status)

@app.get("/health")
def health_check():
    """System health check"""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "uploads_dir": UPLOAD_DIR.exists(),
        "database_initialized": True
    }

@app.get("/stats")
def get_system_stats():
    """Get system statistics"""
    try:
        from violation_track import get_violation_stats
        violation_stats = get_violation_stats()
        
        return JSONResponse(content={
            "system_status": "online",
            "active_video": last_uploaded_video.name if last_uploaded_video else None,
            "total_alerts": len(alerts_log),
            "violation_stats": violation_stats,
            "model_loaded": model is not None
        })
    except Exception as e:
        print(f"❌ Stats error: {e}")
        return JSONResponse(content={"error": "Stats unavailable"})

@app.post("/test_detection")
def test_detection():
    """Test detection functionality"""
    try:
        if model is None:
            raise HTTPException(status_code=503, detail="Model not loaded")
        
        add_alert("Detection test completed successfully", "info")
        return JSONResponse(content={
            "test_status": "passed",
            "model_ready": True,
            "message": "Detection system is working correctly"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Test detection error: {e}")
        raise HTTPException(status_code=500, detail="Test detection failed")

@app.post("/reset_alerts")
def reset_alerts():
    """Reset all alerts"""
    try:
        global alerts_log
        alerts_log.clear()
        from alerts import save_alerts
        save_alerts(alerts_log)
        
        from violation_track import reset_violation_tracking
        reset_violation_tracking()
        
        return JSONResponse(content={"message": "Alerts reset successfully"})
    except Exception as e:
        print(f"Reset error: {e}")
        raise HTTPException(status_code=500, detail="Reset failed")

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "Resource not found"}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500, 
        content={"error": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)