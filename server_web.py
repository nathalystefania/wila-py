# server_web.py
import http.server
import socketserver
import webbrowser
import os

PORT = 1801
FILE = "index.html"

if not os.path.exists(FILE):
    print(f"ERROR: No existe {FILE} en esta carpeta")
    exit(1)

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    url = f"http://localhost:{PORT}/{FILE}"
    print(f"Servidor web corriendo en: {url}")
    webbrowser.open(url)
    httpd.serve_forever()
