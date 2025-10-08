import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Flask App Builder configuration
# Your App secret key
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "your_secret_key_here_change_in_production"
)

# The SQLAlchemy connection string to your database backend
SQLALCHEMY_DATABASE_URI = f"postgresql://{os.environ.get('DATABASE_USER', 'superset')}:{os.environ.get('DATABASE_PASSWORD', 'superset')}@{os.environ.get('DATABASE_HOST', 'superset-db')}:{os.environ.get('DATABASE_PORT', '5432')}/{os.environ.get('DATABASE_DB', 'superset')}"

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# Cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": os.environ.get("REDIS_HOST", "redis"),
    "CACHE_REDIS_PORT": int(os.environ.get("REDIS_PORT", 6379)),
    "CACHE_REDIS_DB": int(os.environ.get("REDIS_DB", 1)),
    "CACHE_REDIS_URL": f"redis://{os.environ.get('REDIS_HOST', 'redis')}:{os.environ.get('REDIS_PORT', 6379)}/1",
}

# Data cache timeout in seconds
DATA_CACHE_CONFIG = CACHE_CONFIG

# Async query configuration
RESULTS_BACKEND = CACHE_CONFIG

# Enable scheduled queries
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Allow embedding
TALISMAN_ENABLED = False
SESSION_COOKIE_SAMESITE = None
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True

# CORS options
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}

# Webdriver configuration for alerts and reports
WEBDRIVER_BASEURL = "http://superset:8088"
WEBDRIVER_BASEURL_USER_FRIENDLY = WEBDRIVER_BASEURL

# Image and file upload configuration
UPLOAD_FOLDER = "/app/superset_home/uploads/"
IMG_UPLOAD_FOLDER = "/app/superset_home/uploads/"
IMG_UPLOAD_URL = "/static/uploads/"

# Default language
BABEL_DEFAULT_LOCALE = "en"

# Async query execution
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
GLOBAL_ASYNC_QUERIES_POLLING_DELAY = 500

# SQL Lab configuration
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
