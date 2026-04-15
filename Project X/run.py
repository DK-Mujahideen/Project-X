"""
Simple launcher for Data Quality Analyzer
Just run: python run.py
"""

import webbrowser
import time
import sys
import subprocess

print("""
╔═══════════════════════════════════════════════════════════╗
║     📊 DATA QUALITY ANALYZER                              ║
║     Starting your application...                         ║
╚═══════════════════════════════════════════════════════════╝
""")

# Start Flask
print("🚀 Starting Flask server...")
flask_process = subprocess.Popen([sys.executable, 'app.py'])

# Wait for server
time.sleep(3)

# Open browser
print("📱 Opening browser...")
webbrowser.open("http://localhost:5000")

print("""
✅ Application is running!

📍 Local Access: http://localhost:5000
📍 Local Access: http://127.0.0.1:5000

🌐 To share with others:
   1. In VS Code, click the 'PORTS' tab
   2. Add port 5000
   3. Right-click → 'Port Visibility' → 'Public'
   4. Copy and share the URL

Press Ctrl+C to stop the server
""")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Stopping server...")
    flask_process.terminate()
    print("✅ Done!")
