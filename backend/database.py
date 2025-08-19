import sqlite3
import threading
from pathlib import Path
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_FILE = Path("safety_alerts.db")
_db_lock = threading.Lock()
_db_initialized = False

def get_db_connection():
    """Get database connection with proper error handling"""
    try:
        conn = sqlite3.connect(str(DB_FILE), timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def init_db():
    """Initialize database with required tables"""
    global _db_initialized
    
    if _db_initialized:
        return True
        
    try:
        with _db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create alerts table with better schema
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'violation',
                    severity INTEGER DEFAULT 1,
                    metadata TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indices for faster queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
                ON alerts(timestamp)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_alerts_status 
                ON alerts(status)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_alerts_created_at 
                ON alerts(created_at)
            ''')
            
            # Create system_stats table for better performance tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_name TEXT UNIQUE NOT NULL,
                    stat_value TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
        _db_initialized = True
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        _db_initialized = False
        raise

def insert_alert(message, status="violation", metadata=None):
    """Insert alert into database with error handling"""
    try:
        if not _db_initialized:
            init_db()
            
        # Validate inputs
        if not message:
            logger.error("Cannot insert alert: message is required")
            return None
            
        message = str(message)[:500]  # Limit message length
        status = str(status).lower() if status else "violation"
        
        with _db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            severity = get_severity_level(status)
            metadata_str = json.dumps(metadata) if metadata else None
            
            cursor.execute('''
                INSERT INTO alerts (timestamp, message, status, severity, metadata)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, message, status, severity, metadata_str))
            
            conn.commit()
            alert_id = cursor.lastrowid
            conn.close()
            
            logger.info(f"Alert inserted with ID {alert_id}: {message}")
            return alert_id
            
    except Exception as e:
        logger.error(f"Error inserting alert: {e}")
        return None

def get_alert_history(limit=50):
    """Get alert history from database"""
    try:
        if not _db_initialized:
            init_db()
            
        if not isinstance(limit, int) or limit <= 0:
            limit = 50
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, message, status, severity, metadata
            FROM alerts 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of tuples for backward compatibility
        history = []
        for row in results:
            history.append((row['timestamp'], row['message'], row['status']))
        
        return history
        
    except Exception as e:
        logger.error(f"Error fetching alert history: {e}")
        return []

def get_alert_details(limit=50):
    """Get detailed alert history including metadata"""
    try:
        if not _db_initialized:
            init_db()
            
        if not isinstance(limit, int) or limit <= 0:
            limit = 50
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, timestamp, message, status, severity, metadata, created_at
            FROM alerts 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        detailed_history = []
        for row in results:
            metadata = None
            if row['metadata']:
                try:
                    metadata = json.loads(row['metadata'])
                except:
                    metadata = row['metadata']
            
            detailed_history.append({
                'id': row['id'],
                'timestamp': row['timestamp'],
                'message': row['message'],
                'status': row['status'],
                'severity': row['severity'],
                'metadata': metadata,
                'created_at': row['created_at']
            })
        
        return detailed_history
        
    except Exception as e:
        logger.error(f"Error fetching detailed alert history: {e}")
        return []

def get_severity_level(status):
    """Get numeric severity level for status"""
    if not isinstance(status, str):
        return 1
        
    severity_map = {
        "violation": 3,
        "danger": 3,
        "critical": 3,
        "warning": 2,
        "caution": 2,
        "info": 1,
        "safe": 0
    }
    return severity_map.get(status.lower(), 1)

def cleanup_old_alerts(days_to_keep=30):
    """Remove alerts older than specified days"""
    try:
        if not _db_initialized:
            init_db()
            
        if not isinstance(days_to_keep, (int, float)) or days_to_keep <= 0:
            days_to_keep = 30
        
        with _db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get count before deletion
            cursor.execute("SELECT COUNT(*) as count FROM alerts")
            original_count = cursor.fetchone()['count']
            
            # Delete old alerts
            cursor.execute('''
                DELETE FROM alerts 
                WHERE datetime(created_at) < datetime('now', '-{} days')
            '''.format(int(days_to_keep)))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old alerts (kept {original_count - deleted_count})")
            
            return deleted_count
            
    except Exception as e:
        logger.error(f"Error cleaning up alerts: {e}")
        return 0

def get_alert_stats():
    """Get alert statistics from database"""
    try:
        if not _db_initialized:
            init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total count
        cursor.execute("SELECT COUNT(*) as total FROM alerts")
        total_result = cursor.fetchone()
        total = total_result['total'] if total_result else 0
        
        # Count by status
        cursor.execute('''
            SELECT status, COUNT(*) as count 
            FROM alerts 
            GROUP BY status
        ''')
        status_results = cursor.fetchall()
        by_status = {row['status']: row['count'] for row in status_results}
        
        # Count by severity
        cursor.execute('''
            SELECT severity, COUNT(*) as count 
            FROM alerts 
            GROUP BY severity
        ''')
        severity_results = cursor.fetchall()
        by_severity_raw = {row['severity']: row['count'] for row in severity_results}
        
        # Map severity numbers to names
        by_severity = {
            "none": by_severity_raw.get(0, 0),
            "low": by_severity_raw.get(1, 0),
            "medium": by_severity_raw.get(2, 0),
            "high": by_severity_raw.get(3, 0)
        }
        
        # Recent alerts (last 24 hours)
        cursor.execute('''
            SELECT COUNT(*) as recent 
            FROM alerts 
            WHERE datetime(created_at) > datetime('now', '-1 day')
        ''')
        recent_result = cursor.fetchone()
        recent_24h = recent_result['recent'] if recent_result else 0
        
        # Recent alerts (last hour)
        cursor.execute('''
            SELECT COUNT(*) as recent_hour 
            FROM alerts 
            WHERE datetime(created_at) > datetime('now', '-1 hour')
        ''')
        recent_hour_result = cursor.fetchone()
        recent_1h = recent_hour_result['recent_hour'] if recent_hour_result else 0
        
        conn.close()
        
        return {
            "total": total,
            "by_status": by_status,
            "by_severity": by_severity,
            "recent_24h": recent_24h,
            "recent_1h": recent_1h
        }
        
    except Exception as e:
        logger.error(f"Error getting alert stats: {e}")
        return {
            "total": 0, 
            "by_status": {}, 
            "by_severity": {"none": 0, "low": 0, "medium": 0, "high": 0},
            "recent_24h": 0,
            "recent_1h": 0
        }

def get_alerts_by_timerange(hours=24):
    """Get alerts within a specific timerange"""
    try:
        if not _db_initialized:
            init_db()
            
        if not isinstance(hours, (int, float)) or hours <= 0:
            hours = 24
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, message, status, severity
            FROM alerts 
            WHERE datetime(created_at) > datetime('now', '-{} hours')
            ORDER BY created_at DESC
        '''.format(hours))
        
        results = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"Error getting alerts by timerange: {e}")
        return []

def get_database_health():
    """Check database health and return status"""
    try:
        health_info = {
            "db_file_exists": DB_FILE.exists(),
            "db_initialized": _db_initialized,
            "db_size_mb": 0,
            "connection_test": False,
            "table_counts": {}
        }
        
        if DB_FILE.exists():
            health_info["db_size_mb"] = round(DB_FILE.stat().st_size / (1024 * 1024), 2)
        
        # Test connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get table counts
        cursor.execute("SELECT COUNT(*) as count FROM alerts")
        alerts_count = cursor.fetchone()['count']
        health_info["table_counts"]["alerts"] = alerts_count
        
        # Test a simple query
        cursor.execute("SELECT 1")
        cursor.fetchone()
        health_info["connection_test"] = True
        
        conn.close()
        
        return health_info
        
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            "db_file_exists": DB_FILE.exists() if DB_FILE else False,
            "db_initialized": False,
            "db_size_mb": 0,
            "connection_test": False,
            "table_counts": {},
            "error": str(e)
        }

def vacuum_database():
    """Vacuum the database to optimize performance"""
    try:
        if not _db_initialized:
            return False
        
        with _db_lock:
            conn = get_db_connection()
            conn.execute("VACUUM;")
            conn.close()
            
        logger.info("Database vacuum completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database vacuum failed: {e}")
        return False

def update_system_stat(stat_name, stat_value):
    """Update or insert a system statistic"""
    try:
        if not _db_initialized:
            init_db()
        
        with _db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO system_stats (stat_name, stat_value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (str(stat_name), str(stat_value)))
            
            conn.commit()
            conn.close()
            
        return True
        
    except Exception as e:
        logger.error(f"Error updating system stat: {e}")
        return False

def get_system_stat(stat_name):
    """Get a specific system statistic"""
    try:
        if not _db_initialized:
            init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT stat_value, updated_at FROM system_stats 
            WHERE stat_name = ?
        ''', (str(stat_name),))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                "value": result['stat_value'],
                "updated_at": result['updated_at']
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting system stat: {e}")
        return None

# Initialize database on import
try:
    init_db()
except Exception as e:
    logger.error(f"Failed to initialize database on import: {e}")