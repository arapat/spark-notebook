import sys


def good_main():
    print "Mock succeed."
    return True


def bad_main():
    print "will fail"
    sys.exit(1)

main = bad_main
