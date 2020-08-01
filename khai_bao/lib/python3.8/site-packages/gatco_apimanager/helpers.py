def upper_keys(d):
    """Returns a new dictionary with the keys of `d` converted to upper case
    and the values left unchanged.

    """
    return dict(zip((k.upper() for k in d.keys()), d.values()))