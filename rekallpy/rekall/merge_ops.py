"""This class contains various useful merge ops."""

def payload_first(payload1, payload2):
    return payload1

def payload_second(payload1, payload2):
    return payload2

def payload_plus(payload1, payload2):
    return payload1 + payload2

def merge_named_payload(name_to_merge_op):
    """Merging dictionary payload by key.

    name_to_merge_op is a dict mapping from field names to merge_ops.

    Example:
        If name_to_merge_op is 
        {
            'f1': mergeop1,
            'f2': mergeop2,
            'f3': mergeop3
        },
        Then two payloads { 'f1': a1, 'f2': b1, 'f3': c1 } and
        { 'f1': a2, 'f2': b2, 'f3': c2 } will be merged into
        {
          'f1': mergeop1(a1, a2),
          'f2': mergeop2(b1, b2),
          'f3': mergeop3(c1, c2)
        }.
    """

    def merge(p1,p2):
        p = {}
        for name, op in name_to_merge_op.items():
            p[name] = op(p1[name], p2[name])
        return p
    return merge
