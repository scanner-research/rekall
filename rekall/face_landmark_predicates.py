""" Face landmark predicates.

Each of these predicates takes in an object that is assumed to have the 
following structure:
{
    'landmarks': obj
}
where obj has the following functions, each of which returns an (N, 2) numpy
array containing all the landmark points:
    face_outline()
    right_eyebrow()
    left_eyebrow()
    nose_bridge()
    nose_bottom()
    right_eye()
    left_eye()
    outer_lips()
    inner_lips()

Simple predicates we have so far:
    looking_left - calculated based on nose direction
    looking_right - calculated based on nose direction
"""

import numpy as np

def looking_left(landmarks):
    """Whether the face is looking to the left.

    Checks whether the bottom landmark on the nose is left of the middle
    of the nose bottom.
    """
    nose_bridge = landmarks['landmarks'].nose_bridge()
    nose_bottom = landmarks['landmarks'].nose_bottom()

    nose_bottom_x_min = np.min(nose_bottom[:,0])
    nose_bottom_x_max = np.max(nose_bottom[:,0])

    return nose_bridge[-1][0] < (nose_bottom_x_min + nose_bottom_x_max) / 2

def looking_right(landmarks):
    """Whether the face is looking to the right.

    Checks whether the bottom landmark on the nose is right of the middle
    of the nose bottom.
    """
    nose_bridge = landmarks['landmarks'].nose_bridge()
    nose_bottom = landmarks['landmarks'].nose_bottom()

    nose_bottom_x_min = np.min(nose_bottom[:,0])
    nose_bottom_x_max = np.max(nose_bottom[:,0])

    return nose_bridge[-1][0] > (nose_bottom_x_min + nose_bottom_x_max) / 2
