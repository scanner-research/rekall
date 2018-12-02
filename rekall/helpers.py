import sys

def intrvl1_payload(intrvl1, intrvl2):
    return intrvl1.payload

def intrvl2_payload(intrvl1, intrvl2):
    return intrvl2.payload

def panic(message):
    """ Print an error message and exit. """
    print(message, file=sys.stderr)
    sys.exit(1)
