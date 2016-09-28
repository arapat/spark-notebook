import socket
import sys
import threading
import webbrowser

debug = False

if __name__ == '__main__':
    if sys.version_info < (3, 4):
        print("Error: Spark-notebook requires Python 3.4 or newer.")
        sys.exit(1)
    from deploy.server import app
    if debug:
        app.run(port=5000, debug=True)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 5000
        while sock.connect_ex(("localhost", port)) == 0:
            # Port is in use
            port = port + 1
        threading.Timer(
            1, lambda: webbrowser.open("http://localhost:%d/" % port)).start()
        app.run(port=port)
