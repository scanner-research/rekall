"""Payload Parsers"""

from functools import reduce

def dict_payload_parser(accessor, fields):
    """Parse an object to generate a payload.

    @accesor takes in object and a field name and gets a value.
    @fields is a dict mapping from field names in the payload to field names in
    the object.
    """
    def parse_object(obj):
        return { field_name: accessor(obj, fields[field_name])
                for field_name in fields }

    return parse_object

def label_payload_parser(accessor, label):
    """ Parser that produces { "label": ... } """
    return dict_payload_parser(accessor, { "label": label })

def bbox_payload_parser(accessor, x1="bbox_x1", y1="bbox_y1", x2="bbox_x2",
        y2="bbox_y2"):
    """Parses an object to generate a bbox payload.
    
    Generates a dict with this structure:
    { "x1": ..., "y1": ..., "x2": ..., "y2": ... }
    @x1, @y1, @x2, @y2 are field names in the object.
    @accessor takes in the object and a field name and gets the value for that
    object.
    """
    return dict_payload_parser(accessor, {
        "x1": x1, "y1": y1, "x2": x2, "y2": y2
    })

def in_array(parser_fn):
    """Generate a new parser function that wraps a payload result in an array.
    """
    return lambda obj: [parser_fn(obj)]

def merge_dict_parsers(parser_fns):
    """Generate a new parser that merges the result of multiple parsers.

    @parser_fns is a list of parser functions. Must parse objects into dicts
    with unique keys.
    """
    # { **obj1, **obj2 } merges the two dicts
    return lambda obj: reduce(lambda obj1, obj2: { **obj1, **obj2 },
            [fn(obj) for fn in parser_fns])

def named_payload(name, parser_fn):
    """Wraps a parser result in a dictionary under given name."""
    return lambda obj: {name: parser_fn(obj)}
