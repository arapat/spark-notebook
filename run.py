#!/usr/bin/env python

import socket
import sys
import threading
import webbrowser

debug = False


def get_available_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = 5000
    tried = 0
    while True:
        tried = tried + 1
        try:
            sock.bind(("127.0.0.1", port))
            sock.close()
            break
        except socket.error as e:
            if tried < 10:
                port = port + 1
            else:
                print(e)
                print("Error: Cannot find an available port after 10 tries.")
                sys.exit(1)
    return port


if __name__ == '__main__':
    from spark_notebook.server import app

    # Find an available port
    port = get_available_port()
    threading.Timer(
        1, lambda: webbrowser.open("http://localhost:%d/" % port)).start()
    app.run(port=port, debug=debug)
