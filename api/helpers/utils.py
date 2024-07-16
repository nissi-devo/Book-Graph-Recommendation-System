from datetime import datetime, timezone, timedelta

def date_to_unix_time(date_str):
    """Converts a date string in format 'YYYY-MM-DD' to Unix time."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    # Ensure dt is timezone-aware (set it to UTC)
    dt_utc = dt.replace(tzinfo=timezone.utc)
    # Calculate Unix time
    return int((dt_utc - epoch).total_seconds())