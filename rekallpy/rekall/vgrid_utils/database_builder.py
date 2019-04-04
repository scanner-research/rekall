import shlex
import subprocess as sp
import json
import os

class DbVideo:
    """Metadata about a video.

    The basic metadata is the video path and ID. The ID can be any arbitrary
    unique number, or a database ID if you have one. If the video path is on
    your local machine (as opposed to, say, a cloud bucket), then this class
    can fill in the remaining metadata (width, height, etc.) using ffprobe.
    """

    def __init__(self, path, id, fps=None, num_frames=None, width=None, height=None):
        if not os.path.isfile(path):
            raise Exception("Error: path {} does not exist".format(path))

        if fps is None:
            cmd = 'ffprobe -v quiet -print_format json -show_streams "{}"' \
                .format(path)
            outp = sp.check_output(shlex.split(cmd))
            streams = json.loads(outp)['streams']
            video_stream = [s for s in streams if s['codec_type'] == 'video'][0]
            width = video_stream['width']
            height = video_stream['height']
            [num, denom] = map(int, video_stream['r_frame_rate'].split('/'))
            fps = float(num) / float(denom)
            num_frames = video_stream['nb_frames']

        self.path = path
        self.id = id
        self.fps = fps
        self.num_frames = num_frames
        self.width = width
        self.height = height

    def to_json(self):
        return {
            'id': self.id,
            'path': self.path,
            'num_frames': self.num_frames,
            'fps': self.fps,
            'width': self.width,
            'height': self.height
        }

class DbCategory:
    def __init__(self):
        # TODO
        pass

class DatabaseBuilder:
    """Builder for the VGrid metadata database."""

    def __init__(self, video_baseurl):
        self._video_baseurl = video_baseurl
        self._videos = None

    def add_videos(self, videos):
        """Adds a list of videos the database.

        Args:
            videos (List[DbVideo]): the videos to add

        Returns:
            self for chaining
        """
        self._videos = videos
        return self

    def add_category(self, category):
        # TODO
        return self

    def build(self):
        for video in self._videos:
            video.path = '{}/{}'.format(self._video_baseurl, video.path)

        return {
            'videos': [v.to_json() for v in self._videos]
        }

