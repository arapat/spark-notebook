import sys
from time import sleep


def main():
    for k in range(3):
        print k
        sleep(1)
    print sys.argv
    print "Failed!"
    sys.exit(1)
