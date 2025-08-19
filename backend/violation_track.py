from collections import defaultdict, deque
import time
import threading
from alerts import add_alert, alerts_log
from alerts_categories import CLASS_NAMES
from database import insert_alert
import logging

logger = logging.getLogger(__name__)

# Thread-safe violation memory
violation_memory = defaultdict(lambda: deque(maxlen=20))
_violation_lock = threading.Lock()

# Configurable thresholds
ALERT_THRESHOLD = 3  # Number of repeated detections required
ROLLING_WINDOW_SECONDS = 10  # Time window in seconds
COOLDOWN_SECONDS = 30  # Cooldown between alerts for same violation

# Last alert times for cooldown
last_alert_times = defaultdict(float)

def configure_thresholds(alert_threshold=None, window_seconds=None, cooldown_seconds=None):
    """Configure violation tracking thresholds"""
    global ALERT_THRESHOLD, ROLLING_WINDOW_SECONDS, COOLDOWN_SECONDS
    
    if alert_threshold is not None:
        ALERT_THRESHOLD = max(1, alert_threshold)
    if window_seconds is not None:
        ROLLING_WINDOW_SECONDS = max(1, window_seconds)
    if cooldown_seconds is not None:
        COOLDOWN_SECONDS = max(0, cooldown_seconds)
    
    logger.info(f"Thresholds updated: alert={ALERT_THRESHOLD}, window={ROLLING_WINDOW_SECONDS}s, cooldown={COOLDOWN_SECONDS}s")

def track_violation(cls_id):
    """Tracks violations and triggers an alert if threshold is met."""
    if cls_id not in CLASS_NAMES:
        return False
    
    now = time.time()
    
    with _violation_lock:
        memory = violation_memory[cls_id]
        memory.append(now)
        
        # Remove timestamps older than the rolling window
        while memory and now - memory[0] > ROLLING_WINDOW_SECONDS:
            memory.popleft()
        
        # Check if threshold is met and cooldown has passed
        if len(memory) >= ALERT_THRESHOLD:
            last_alert_time = last_alert_times[cls_id]
            
            if now - last_alert_time > COOLDOWN_SECONDS:
                violation_name = CLASS_NAMES.get(cls_id, f"Unknown violation {cls_id}")
                
                # Create detailed alert message
                alert_message = f"PPE Violation Detected: {violation_name}"
                
                # Check if this is a different alert than the last one
                if not alerts_log or alerts_log[-1]["message"] != alert_message:
                    try:
                        # Add to memory alerts
                        add_alert(alert_message, "violation", {
                            "class_id": cls_id,
                            "detection_count": len(memory),
                            "time_window": ROLLING_WINDOW_SECONDS
                        })
                        
                        # Add to database
                        insert_alert(alert_message, "violation")
                        
                        # Update cooldown
                        last_alert_times[cls_id] = now
                        
                        logger.warning(f"VIOLATION ALERT: {alert_message}")
                        return True
                        
                    except Exception as e:
                        logger.error(f"Error tracking violation: {e}")
                        return False
    
    return False

def get_violation_stats():
    """Get current violation statistics"""
    with _violation_lock:
        stats = {}
        now = time.time()
        
        for cls_id, memory in violation_memory.items():
            # Clean old entries
            while memory and now - memory[0] > ROLLING_WINDOW_SECONDS:
                memory.popleft()
            
            if memory:  # Only include active violations
                violation_name = CLASS_NAMES.get(cls_id, f"Class_{cls_id}")
                stats[violation_name] = {
                    "recent_detections": len(memory),
                    "threshold": ALERT_THRESHOLD,
                    "last_detection": memory[-1] if memory else 0,
                    "time_to_alert": max(0, ALERT_THRESHOLD - len(memory))
                }
        
        return {
            "active_violations": stats,
            "config": {
                "alert_threshold": ALERT_THRESHOLD,
                "window_seconds": ROLLING_WINDOW_SECONDS,
                "cooldown_seconds": COOLDOWN_SECONDS
            },
            "total_tracked_classes": len(violation_memory)
        }

def reset_violation_tracking():
    """Reset all violation tracking data"""
    with _violation_lock:
        violation_memory.clear()
        last_alert_times.clear()
    
    logger.info("Violation tracking data reset")

def get_active_violations():
    """Get currently active violations (within time window)"""
    active = []
    now = time.time()
    
    with _violation_lock:
        for cls_id, memory in violation_memory.items():
            # Clean old entries
            while memory and now - memory[0] > ROLLING_WINDOW_SECONDS:
                memory.popleft()
            
            if memory:
                violation_name = CLASS_NAMES.get(cls_id, f"Class_{cls_id}")
                active.append({
                    "class_id": cls_id,
                    "name": violation_name,
                    "detection_count": len(memory),
                    "progress": len(memory) / ALERT_THRESHOLD,
                    "last_seen": memory[-1]
                })
    
    return active

def is_violation_active(cls_id):
    """Check if a specific violation is currently active"""
    now = time.time()
    
    with _violation_lock:
        memory = violation_memory[cls_id]
        
        # Clean old entries
        while memory and now - memory[0] > ROLLING_WINDOW_SECONDS:
            memory.popleft()
        
        return len(memory) > 0

# Cleanup function to prevent memory leaks
def cleanup_old_violations():
    """Remove old violation data to prevent memory leaks"""
    now = time.time()
    removed_count = 0
    
    with _violation_lock:
        classes_to_remove = []
        
        for cls_id, memory in violation_memory.items():
            # Clean old entries
            while memory and now - memory[0] > ROLLING_WINDOW_SECONDS * 2:  # Keep 2x window for history
                memory.popleft()
                removed_count += 1
            
            # Remove empty memories
            if not memory:
                classes_to_remove.append(cls_id)
        
        # Remove empty violation memories
        for cls_id in classes_to_remove:
            del violation_memory[cls_id]
            if cls_id in last_alert_times:
                del last_alert_times[cls_id]
    
    if removed_count > 0 or classes_to_remove:
        logger.info(f"Cleaned up {removed_count} old detections and {len(classes_to_remove)} empty classes")
    
    return removed_count

# Auto-cleanup timer (can be called periodically)
import atexit

def _cleanup_on_exit():
    """Cleanup function called on program exit"""
    try:
        cleanup_old_violations()
        logger.info("Violation tracking cleaned up on exit")
    except:
        pass

atexit.register(_cleanup_on_exit)