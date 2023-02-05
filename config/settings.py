import os


SECRET_KEY = os.getenv("SECRET_KEY", None)

# SERVER_NAME = os.getenv(
#     "SERVER_NAME", "localhost:{0}".format(os.getenv("PORT", "8000"))
# )
# SQLAlchemy.
mariadb_user = os.getenv("MARIADB_USER", "allthethings")
mariadb_password = os.getenv("MARIADB_PASSWORD", "password")
mariadb_host = os.getenv("MARIADB_HOST", "mariadb")
mariadb_port = os.getenv("MARIADB_PORT", "3306")
mariadb_db = os.getenv("MARIADB_DATABASE", mariadb_user)
mariadb_url = f"mysql+pymysql://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}"
SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", mariadb_url)
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_POOL_SIZE = 100
SQLALCHEMY_MAX_OVERFLOW = -1
SQLALCHEMY_ENGINE_OPTIONS = { 'isolation_level': 'AUTOCOMMIT' }

mariapersist_user = os.getenv("MARIAPERSIST_USER", "allthethings")
mariapersist_password = os.getenv("MARIAPERSIST_PASSWORD", "password")
mariapersist_host = os.getenv("MARIAPERSIST_HOST", "mariapersist")
mariapersist_port = os.getenv("MARIAPERSIST_PORT", "3333")
mariapersist_db = os.getenv("MARIAPERSIST_DATABASE", mariapersist_user)
mariapersist_url = f"mysql+pymysql://{mariapersist_user}:{mariapersist_password}@{mariapersist_host}:{mariapersist_port}/{mariapersist_db}"

SQLALCHEMY_BINDS = {
    'mariapersist': mariapersist_url,
}

# Redis.
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Celery.
CELERY_CONFIG = {
    "broker_url": REDIS_URL,
    "result_backend": REDIS_URL,
    "include": [],
}

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
