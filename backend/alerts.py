import json
import threading
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALERTS_FILE = Path("alerts_log.json")
_alerts_lock = threading.Lock()

def load_alerts():
    """Load alerts from JSON file with error handling"""
    if ALERTS_FILE.exists():
        try:
            with open(ALERTS_FILE, "r", encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    return []
                
                alerts = json.loads(content)
                
                # Validate alert structure
                valid_alerts = []
                required_keys = ['time', 'message', 'status']
                
                for alert in alerts:
                    if isinstance(alert, dict) and all(key in alert for key in required_keys):
                        # Ensure all required fields are strings (except metadata)
                        if all(isinstance(alert[key], str) for key in required_keys):
                            valid_alerts.append(alert)
                        else:
                            logger.warning(f"Invalid alert data types: {alert}")
                    else:
                        logger.warning(f"Invalid alert format: {alert}")
                
                return valid_alerts[-1000:]  # Keep only last 1000 alerts
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in alerts file: {e}")
            # Backup corrupted file
            backup_path = ALERTS_FILE.with_suffix('.json.backup')
            try:
                ALERTS_FILE.rename(backup_path)
                logger.info(f"Corrupted file backed up to: {backup_path}")
            except Exception as backup_error:
                logger.error(f"Failed to backup corrupted file: {backup_error}")
            return []
        except Exception as e:
            logger.error(f"Error loading alerts: {e}")
            return []
    return []

def save_alerts(alerts):
    """Save alerts to JSON file with thread safety"""
    with _alerts_lock:
        try:
            # Ensure directory exists
            ALERTS_FILE.parent.mkdir(exist_ok=True)
            
            # Validate alerts before saving
            valid_alerts = []
            required_keys = ['time', 'message', 'status']
            
            for alert in alerts:
                if isinstance(alert, dict) and all(key in alert for key in required_keys):
                    # Clean up alert data
                    clean_alert = {
                        'time': str(alert['time']),
                        'message': str(alert['message']),
                        'status': str(alert['status'])
                    }
                    
                    # Add optional fields if they exist
                    for optional_key in ['id', 'timestamp', 'severity', 'metadata']:
                        if optional_key in alert:
                            clean_alert[optional_key] = alert[optional_key]
                    
                    valid_alerts.append(clean_alert)
                else:
                    logger.warning(f"Skipping invalid alert during save: {alert}")
            
            # Write to temporary file first for atomic operation
            temp_file = ALERTS_FILE.with_suffix('.tmp')
            with open(temp_file, "w", encoding='utf-8') as f:
                json.dump(valid_alerts, f, indent=2, ensure_ascii=False)
            
            # Replace original file atomically
            temp_file.replace(ALERTS_FILE)
            logger.debug(f"Successfully saved {len(valid_alerts)} alerts")
            
        except Exception as e:
            logger.error(f"Error saving alerts: {e}")
            # Clean up temp file if it exists
            temp_file = ALERTS_FILE.with_suffix('.tmp')
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass

# Load alerts on module import with error handling
try:
    alerts_log = load_alerts()
    logger.info(f"Loaded {len(alerts_log)} existing alerts")
except Exception as e:
    logger.error(f"Failed to load alerts on startup: {e}")
    alerts_log = []

def add_alert(message, status="violation", metadata=None):
    """
    Add a new alert with improved error handling and metadata support
    
    Args:
        message (str): Alert message
        status (str): Alert status (violation, warning, info, safe)
        metadata (dict): Optional additional data
    
    Returns:
        dict: The created alert or None if failed
    """
    try:
        # Validate inputs
        if not message or not isinstance(message, (str, int, float)):
            logger.error("Alert message is required and must be a string/number")
            return None
        
        if not status:
            status = "violation"
        
        # Ensure status is valid
        valid_statuses = ["violation", "warning", "info", "safe", "danger", "critical", "caution"]
        if status.lower() not in valid_statuses:
            logger.warning(f"Unknown status '{status}', using 'violation'")
            status = "violation"
        
        current_time = datetime.now()
        
        alert = {
            "id": f"alert_{current_time.timestamp()}",  # Unique ID
            "time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": current_time.timestamp(),  # For sorting/filtering
            "message": str(message).strip()[:500],  # Limit message length
            "status": status.lower(),
            "severity": get_severity_level(status),
            "metadata": metadata or {}
        }
        
        # Thread-safe append
        with _alerts_lock:
            alerts_log.append(alert)
            
            # Keep only recent alerts to prevent memory issues
            if len(alerts_log) > 1000:
                alerts_log[:] = alerts_log[-1000:]
        
        # Save to file in background to avoid blocking
        try:
            save_alerts(alerts_log)
        except Exception as save_error:
            logger.error(f"Failed to save alerts: {save_error}")
        
        # Log the alert
        severity = alert["severity"]
        if severity >= 3:  # High severity
            logger.error(f"HIGH SEVERITY ALERT: {message}")
        elif severity >= 2:  # Medium severity
            logger.warning(f"MEDIUM SEVERITY ALERT: {message}")
        else:
            logger.info(f"ALERT: {message}")
        
        return alert
        
    except Exception as e:
        logger.error(f"Error adding alert: {e}")
        return None

def get_severity_level(status):
    """Get numeric severity level for status"""
    if not isinstance(status, str):
        return 1
        
    severity_map = {
        "violation": 3,  # High
        "danger": 3,
        "critical": 3,
        "warning": 2,    # Medium  
        "caution": 2,
        "info": 1,       # Low
        "safe": 0        # None
    }
    return severity_map.get(status.lower(), 1)

def get_alerts_by_status(status=None, limit=None):
    """Get alerts filtered by status"""
    try:
        with _alerts_lock:
            filtered_alerts = alerts_log.copy()
            
            if status:
                filtered_alerts = [alert for alert in filtered_alerts if alert.get('status') == status.lower()]
            
            if limit and isinstance(limit, int) and limit > 0:
                filtered_alerts = filtered_alerts[-limit:]
            
            return filtered_alerts
    except Exception as e:
        logger.error(f"Error filtering alerts by status: {e}")
        return []

def get_recent_alerts(hours=24):
    """Get alerts from the last N hours"""
    try:
        if not isinstance(hours, (int, float)) or hours <= 0:
            hours = 24
            
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cutoff_timestamp = cutoff_time.timestamp()
        
        with _alerts_lock:
            recent_alerts = [
                alert for alert in alerts_log 
                if alert.get('timestamp', 0) >= cutoff_timestamp
            ]
            return recent_alerts.copy()
    except Exception as e:
        logger.error(f"Error getting recent alerts: {e}")
        return []

def clear_alerts():
    """Clear all alerts"""
    try:
        with _alerts_lock:
            alerts_log.clear()
        save_alerts(alerts_log)
        logger.info("All alerts cleared")
        return True
    except Exception as e:
        logger.error(f"Error clearing alerts: {e}")
        return False

def get_alert_stats():
    """Get alert statistics"""
    try:
        with _alerts_lock:
            if not alerts_log:
                return {
                    "total": 0,
                    "by_status": {},
                    "by_severity": {"none": 0, "low": 0, "medium": 0, "high": 0},
                    "recent_24h": 0
                }
            
            # Count by status
            status_counts = {}
            severity_counts = {0: 0, 1: 0, 2: 0, 3: 0}
            
            # Recent alerts (last 24 hours)
            recent_cutoff = (datetime.now().timestamp() - 86400)  # 24 hours ago
            recent_count = 0
            
            for alert in alerts_log:
                status = alert.get('status', 'unknown')
                severity = alert.get('severity', 1)
                timestamp = alert.get('timestamp', 0)
                
                status_counts[status] = status_counts.get(status, 0) + 1
                if severity in severity_counts:
                    severity_counts[severity] += 1
                
                if timestamp >= recent_cutoff:
                    recent_count += 1
            
            return {
                "total": len(alerts_log),
                "by_status": status_counts,
                "by_severity": {
                    "none": severity_counts[0],
                    "low": severity_counts[1], 
                    "medium": severity_counts[2],
                    "high": severity_counts[3]
                },
                "recent_24h": recent_count
            }
    except Exception as e:
        logger.error(f"Error getting alert stats: {e}")
        return {
            "total": 0,
            "by_status": {},
            "by_severity": {"none": 0, "low": 0, "medium": 0, "high": 0},
            "recent_24h": 0
        }

def cleanup_old_alerts(days_to_keep=30):
    """Remove alerts older than specified days"""
    try:
        if not isinstance(days_to_keep, (int, float)) or days_to_keep <= 0:
            days_to_keep = 30
            
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(days=days_to_keep)
        cutoff_timestamp = cutoff_time.timestamp()
        
        with _alerts_lock:
            original_count = len(alerts_log)
            alerts_log[:] = [
                alert for alert in alerts_log 
                if alert.get('timestamp', float('inf')) >= cutoff_timestamp
            ]
            removed_count = original_count - len(alerts_log)
        
        if removed_count > 0:
            save_alerts(alerts_log)
            logger.info(f"Cleaned up {removed_count} old alerts")
        
        return removed_count
        
    except Exception as e:
        logger.error(f"Error cleaning up old alerts: {e}")
        return 0

# Auto-cleanup on module load if needed
try:
    if len(alerts_log) > 5000:  # If too many alerts, clean up
        cleanup_old_alerts(7)  # Keep only last week
        logger.info("Performed automatic cleanup of old alerts")
except Exception as e:
    logger.error(f"Auto-cleanup failed: {e}")

# Health check function
def check_alerts_health():
    """Check the health of the alerts system"""
    try:
        return {
            "alerts_file_exists": ALERTS_FILE.exists(),
            "alerts_count": len(alerts_log),
            "can_write": True,  # Will be set to False if save fails
            "last_alert": alerts_log[-1] if alerts_log else None
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "alerts_file_exists": False,
            "alerts_count": 0,
            "can_write": False,
            "last_alert": None,
            "error": str(e)
        }