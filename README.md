# PPE-SafetyGuard 

AI-powered PPE (Personal Protective Equipment) detection system for workplace safety.  



## 🏗 Tech Stack
- **YOLOv8** – PPE detection model
- **OpenCV** – Video capture & processing
- **FastAPI** – Backend API & inference
- **Uvicorn** – ASGI server for running FastAPI
- **HTML, JavaScript, CSS** – Frontend web dashboard



## 🚀 Features
- Detects helmets, vests, gloves, boots, and goggles in real-time.
- FastAPI backend for AI inference.
- Web dashboard to monitor alerts and detections.



## ⚡ Installation (Quick Start)
1. Clone the repo:
   ```
   git clone https://github.com/yourusername/SafetyGuard.git
   cd SafetyGuard
   ```
2. Install backend dependencies:
  ```
  cd backend
  pip install -r requirements.txt
  ```
3. Start FastAPI server:
  ```
  uvicorn main:app --reload
  ```
  
   


