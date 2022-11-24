from flask import Blueprint

from allthethings.extensions import db
from allthethings.initializers import redis


up = Blueprint("up", __name__, template_folder="templates", url_prefix="/up")


@up.get("/")
def index():
    return ""


@up.get("/databases")
def databases():
    redis.ping()
    db.engine.execute("SELECT 1")
    return ""
