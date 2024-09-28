
# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
# Your App secret key will be used for securely signing the session cookie
# and encrypting sensitive information on the database
# Make sure you are changing this key for your deployment with a strong key.
# Alternatively you can set it with `SUPERSET_SECRET_KEY` environment variable.
# You MUST set this for production environments or the server will refuse
# to start and you will see an error in the logs accordingly.
SECRET_KEY='biIZ6Igzi4A7Rrr83ixWtgGEyWY96J7IAJQfY909E/wbT3A7RNUtOR/1'

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''


# See https://github.com/apache/superset/issues/28336
import os
# Gets the directory of the current script file
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, "superset.db"))
SQLALCHEMY_DATABASE_URI = f"sqlite:///{DB_PATH}?check_same_thread=false"
print(f"SQLite database path: {DB_PATH}")
PREVENT_UNSAFE_DB_CONNECTIONS = False

