import os
import sys
import threading
import webbrowser
from flask import Flask

from deploy.server import app

if __name__ == '__main__':
    #threading.Timer(
    #    1, lambda: webbrowser.open("http://localhost:5000/")).start()

    app.run(debug=True, port=5000)
    #app.run(port=5000)
