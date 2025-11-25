#!/usr/bin/env python3
"""
Main Entry (React-style App.js)

- Loads Flask routes
- Forwards requests to async slot logic
- Uses run_sync() to safely execute async code
- Imports modular components cleanly
"""

import logging
from flask import Flask, jsonify

# Hybrid async executor
from components.part1_async_http import run_sync

# Core wake handler (async)
from components.part4_slot_logic import handle_awake_async

# -----------------------------------------
# Logging
# -----------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-x-bot")

# -----------------------------------------
# Flask App (Sync layer)
# -----------------------------------------
app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "service": "reddit → x bot",
        "mode": "async+sync hybrid",
    })


@app.route("/awake", methods=["GET", "POST"])
def awake():
    """
    Flask is sync, but our engine is async.
    run_sync() safely runs async code inside a sync context.
    """
    try:
        result = run_sync(handle_awake_async())
        return jsonify(result)

    except Exception as e:
        logger.error("Error in /awake: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# -----------------------------------------
# Main server
# -----------------------------------------
if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", "5000"))
    logger.info(f"Starting reddit-x-bot server on port {port}")
    app.run(host="0.0.0.0", port=port)
