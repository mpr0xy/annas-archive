import os


SECRET_KEY = os.getenv("SECRET_KEY", None)

# SERVER_NAME = os.getenv(
#     "SERVER_NAME", "localhost:{0}".format(os.getenv("PORT", "8000"))
# )
# SQLAlchemy.
mysql_user = os.getenv("MARIADB_USER", "allthethings")
mysql_pass = os.getenv("MARIADB_PASSWORD", "password")
mysql_host = os.getenv("MARIADB_HOST", "mariadb")
mysql_port = os.getenv("MARIADB_PORT", "3306")
mysql_db = os.getenv("MARIADB_DATABASE", mysql_user)
db = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_db}"
SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", db)
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_POOL_SIZE = 100
SQLALCHEMY_MAX_OVERFLOW = -1
SQLALCHEMY_ENGINE_OPTIONS = { 'isolation_level': 'AUTOCOMMIT' }

# Redis.
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Celery.
CELERY_CONFIG = {
    "broker_url": REDIS_URL,
    "result_backend": REDIS_URL,
    "include": [],
}

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
