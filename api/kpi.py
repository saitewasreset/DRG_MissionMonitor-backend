from flask import Blueprint, current_app
from api.db import get_db

bp = Blueprint("kpi", __name__, url_prefix="/kpi")

@bp.route("", methods=["GET"])
def get_kpi_info():
    return {
        "code": 200,
        "message": "Success",
        "data": current_app.config["kpi"]
    }