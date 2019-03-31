import sys

def panic(message):
    """ Print an error message and exit. """
    print(message, file=sys.stderr)
    sys.exit(1)
