import threading
import webbrowser

from deploy.server import app

debug = True

if __name__ == '__main__':
    if debug:
        app.run(port=5000, debug=True)
    else:
        threading.Timer(
            1, lambda: webbrowser.open("http://localhost:5000/")).start()
        app.run(port=5000)
