import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
import json
import os
from datetime import datetime
import httpx  # For making HTTP requests
import pytz  # For working with time zones

# --------------------------------------------------
# Setup Logging
# --------------------------------------------------
with open("./processing/log_conf.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")

# --------------------------------------------------
# Load Application Configuration
# --------------------------------------------------
with open("./processing/app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

# Use the datastore filename from the configuration (e.g., event_stats.json)
STATS_FILE = app_config["datastore"]["filename"]

# --------------------------------------------------
# Default Statistics
# --------------------------------------------------
# Note: Using float("inf") for minimum values; these will be replaced when the first valid event is processed.
DEFAULT_STATS = {
    "num_flight_schedules": 0,
    "num_passenger_checkins": 0,
    "max_luggage_weight": 0,
    "min_luggage_weight": float("inf"),
    "max_flight_duration": 0,
    "min_flight_duration": float("inf"),
    "last_updated": "2000-01-01 00:00:00"
}

# --------------------------------------------------
# Helper Functions for Stats File Handling
# --------------------------------------------------
def read_existing_stats():
    """Read existing statistics from JSON file or return default stats."""
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r") as file:
            return json.load(file)
    return DEFAULT_STATS.copy()

def write_stats(stats):
    """Write updated statistics to JSON file."""
    with open(STATS_FILE, "w") as file:
        json.dump(stats, file, indent=4)

# --------------------------------------------------
# GET /stats Endpoint Function
# --------------------------------------------------
def get_stats():
    """Retrieve processed event statistics from JSON file."""
    logger.info("GET request received for /stats")

    if not os.path.exists(STATS_FILE):
        logger.error("Statistics file not found.")
        return {"message": "Statistics do not exist"}, 404

    with open(STATS_FILE, "r") as f:
        event_stats = json.load(f)

    logger.debug(f"Statistics data: {event_stats}")
    logger.info("GET request for /stats completed successfully.")
    return event_stats, 200

# --------------------------------------------------
# Periodic Processing Function
# --------------------------------------------------
def populate_stats():
    """Fetch events from the storage service, update statistics, and save them to the JSON file."""
    logger.info("Periodic processing started.")
    
    # Read current stats from JSON file (or default values if not present)
    stats = read_existing_stats()

    last_updated_timestamp = stats["last_updated"]
    last_updated = datetime.strptime(last_updated_timestamp, "%Y-%m-%d %H:%M:%S")
    
    now = datetime.now(pytz.utc) 

    start_timestamp = last_updated.strftime("%Y-%m-%d %H:%M:%S")
    end_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Use storage service URLs from configuration
    flight_url = app_config["eventstores"]["flight_schedules"]["url"]
    checkin_url = app_config["eventstores"]["passenger_checkins"]["url"]

    # Append query parameters for the time range
    flight_query_url = f"{flight_url}?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}"
    checkin_query_url = f"{checkin_url}?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}"

    # Initialize empty event lists in case the request fails
    flight_events = []
    checkin_events = []

    # Query the storage service for flight schedule events
    flight_response = httpx.get(flight_query_url)
    if flight_response.status_code == 200:
        flight_events = flight_response.json()
        logger.info(f"Received {len(flight_events)} flight schedule events.")
    else:
        logger.error(f"Error fetching flight schedules: {flight_response.status_code}")

    # Query the storage service for passenger check-in events
    checkin_response = httpx.get(checkin_query_url)
    if checkin_response.status_code == 200:
        checkin_events = checkin_response.json()
        logger.info(f"Received {len(checkin_events)} passenger check-in events.")
    else:
        logger.error(f"Error fetching passenger check-ins: {checkin_response.status_code}")

    # Update cumulative counts
    stats["num_flight_schedules"] += len(flight_events)
    stats["num_passenger_checkins"] += len(checkin_events)

    # Update flight duration statistics from flight events
    for event in flight_events:
        duration = event.get("flight_duration")
        if duration is not None:
            stats["max_flight_duration"] = max(stats["max_flight_duration"], duration)
            stats["min_flight_duration"] = min(stats["min_flight_duration"], duration)

    # Update luggage weight statistics from passenger check-in events
    for event in checkin_events:
        weight = event.get("luggage_weight")
        if weight is not None:
            stats["max_luggage_weight"] = max(stats["max_luggage_weight"], weight)
            stats["min_luggage_weight"] = min(stats["min_luggage_weight"], weight)

    # Update last_updated to the most recent of the new event timestamps and the current end_timestamp.
    stats["last_updated"] = now.strftime("%Y-%m-%d %H:%M:%S")

    # Save the updated statistics to the JSON file
    write_stats(stats)
    logger.debug(f"Updated statistics: {stats}")
    logger.info("Periodic processing ended.")

# --------------------------------------------------
# Scheduler Initialization
# --------------------------------------------------
def init_scheduler():
    """Initialize the scheduler to call populate_stats periodically."""
    sched = BackgroundScheduler(daemon=True)
    # Use the scheduler interval from the configuration (in seconds)
    interval = app_config["scheduler"]["interval"]
    sched.add_job(populate_stats, "interval", seconds=interval)
    sched.start()

# --------------------------------------------------
# Set Up the API with Connexion
# --------------------------------------------------
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

# --------------------------------------------------
# Main Execution
# --------------------------------------------------
if __name__ == "__main__":
    init_scheduler()
    logger.info("Starting processing service on port 8100")
    app.run(port=8100)
