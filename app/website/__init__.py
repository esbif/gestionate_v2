from flask import Blueprint

bp = Blueprint('website', __name__)

from app.website import routes
