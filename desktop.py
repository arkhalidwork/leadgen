"""
LeadGen Desktop App
Launches the Flask app inside a native desktop window using pywebview.
Works both in development and when frozen by PyInstaller.
"""

import os
import sys
import threading
import webview


def get_resource_path(relative_path):
    """Get absolute path to resource, works for dev and PyInstaller bundle."""
    if getattr(sys, "frozen", False):
        # Running as PyInstaller bundle
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)


# Patch Flask's template and static folders before importing app
os.environ["LEADGEN_TEMPLATE_DIR"] = get_resource_path("templates")
os.environ["LEADGEN_STATIC_DIR"] = get_resource_path("static")

# Ensure output dir exists (writable location outside bundle)
if getattr(sys, "frozen", False):
    output_dir = os.path.join(os.path.expanduser("~"), "LeadGen_Output")
else:
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(output_dir, exist_ok=True)
os.environ["LEADGEN_OUTPUT_DIR"] = output_dir

# Desktop mode — skip landing page, go straight to login/dashboard
os.environ["LEADGEN_DESKTOP"] = "1"

from app import app


def start_flask():
    """Run Flask in a background thread (no browser auto-open)."""
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)


if __name__ == "__main__":
    # Start Flask server in a background thread
    server = threading.Thread(target=start_flask, daemon=True)
    server.start()

    # Open a native desktop window pointing at the Flask app
    webview.create_window(
        title="LeadGen Suite",
        url="http://127.0.0.1:5000",
        width=1400,
        height=900,
        min_size=(900, 600),
    )
    webview.start()
