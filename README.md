# rekall: compositional video event specification

[![Build Status](https://travis-ci.com/scanner-research/rekall.svg?branch=master)](https://travis-ci.com/scanner-research/rekall)
[![Documentation Status](https://readthedocs.org/projects/rekallpy/badge/?version=latest)](https://rekallpy.readthedocs.io/en/latest/?badge=latest)

Rekall is a library for compositional video event specification.
We use Rekall to detect new events in video -- such as interviews and
commercials in TV news broadcasts, or action sequences in Hollywood films -- by
composing the outputs of pre-trained computer vision models.

<div>
  <span>
    <img src="figs/interview_clip2.gif" width="200">
  </span>
  <span>
    <img src="figs/commercials1.gif" width="200">
  </span>
  <span>
    <img src="figs/action_sequence1.gif" width="200">
  </span>
  <span>
    <img src="figs/parking_spot1.gif" width="200">
  </span>
</div>

Check out our [tech report](http://www.danfu.org/projects/rekall-tech-report/)
for more details and demo videos.

Rekall has a main [Python API](https://github.com/scanner-research/rekall/tree/master/rekallpy)
for all the core interval processing operations. Rekall also has a
[Javascript API](https://github.com/scanner-research/rekall/tree/master/rekalljs)
which we use for the [vgrid](https://github.com/scanner-research/vgrid) video
metadata visualization widget.

## Getting Started
* Quickly [install](#installation) Rekall
* Try out the [tutorials](tutorials/)
* Check out the [documentation](https://rekallpy.readthedocs.io/en/latest/?badge=latest)
* View the [developer guide](#developer-guidelines)

## Sample Usage
Rekall provides utilities for processing spatiotemporal intervals (like bounding
boxes in a video).
This code sample shows how bounding boxes for a few videos can be loaded into
Rekall:
```python
from rekall import Interval, IntervalSet, IntervalSetMapping, Bounds3D
import urllib3, requests, os

urllib3.disable_warnings()
VIDEO_COLLECTION_BASEURL = "http://olimar.stanford.edu/hdd/rekall_tutorials/cydet/" 
VIDEO_METADATA_FILENAME = "metadata.json"
req = requests.get(os.path.join(VIDEO_COLLECTION_BASEURL, VIDEO_METADATA_FILENAME), verify=False)
video_collection = sorted(req.json(), key=lambda vm: vm['filename'])

video_metadata = [
    VideoMetadata(v["filename"], v["id"], v["fps"], int(v["num_frames"]), v["width"], v["height"])
    for v in video_collection
]

maskrcnn_bbox_files = [ 'maskrcnn_bboxes_0001.pkl', 'maskrcnn_bboxes_0004.pkl' ]

maskrcnn_bboxes = []
for bbox_file in maskrcnn_bbox_files:
    req = requests.get(os.path.join(VIDEO_COLLECTION_BASEURL, bbox_file), verify=False)
    maskrcnn_bboxes.append(pickle.loads(req.content))

# Load Mask R-CNN data into Rekall
maskrcnn_bboxes_ism = IntervalSetMapping({
    vm.id: IntervalSet([
        Interval(
            Bounds3D(
                t1 = frame_num / vm.fps,
                t2 = (frame_num + 1) / vm.fps,
                x1 = bbox[0] / vm.width,
                x2 = bbox[2] / vm.width,
                y1 = bbox[1] / vm.height,
                y2 = bbox[3] / vm.height
            ),
            payload = {
                'class': bbox[4],
                'score': bbox[5]
            }
        )
        for frame_num, bboxes_in_frame in enumerate(maskrcnn_frame_list)
        for bbox in bboxes_in_frame
    ])
    for vm, maskrcnn_frame_list in zip(video_metadata, maskrcnn_bboxes)
})
```
Check out the [tutorials](tutorials/) for more on how Rekall can be used to
operate on this spatiotemporal data.

## Installation

### Python API
Rekall requires Python 3.5 or greater.
```
pip3 install rekallpy
```

### JavaScript API
The Rekall JavaScript API must be installed in the context of a JavaScript
application using the [npm package structure](https://docs.npmjs.com/about-packages-and-modules).
You must have [npm](https://www.npmjs.com/get-npm) installed.
```
npm install --save @wcrichto/rekall
```

Now that you've installed Rekall, check out the [tutorials](tutorials/)!

## Developer Guidelines
If you are interested in contributing to Rekall (and we welcome contribution
via pull requests!), you should install Rekall from source:

[1] Clone the rekall repo
```
git clone https://github.com/scanner-research/rekall
```

[2] Install Python API from source
```
cd rekall/rekallpy
pip3 install -e .
```

And run tests:
```
python3 -m unittest discover test
```

[3] Install JavaScript API from source
```
cd rekall/rekalljs
npm install
npm run prepublishOnly
npm link
```

## Citation

If you used Rekall or found it useful for you research, please cite our [arXiv paper](https://arxiv.org/abs/1910.02993):

```
@article{fu2019rekall,
  author = {Daniel Y. Fu and Will Crichton and James Hong and Xinwei Yao and Haotian Zhang and Anh Truong and Avanika Narayan and Maneesh Agrawala and Christopher R\'e and Kayvon Fatahalian},
  title = {Rekall: Specifying Video Events using Compositions of Spatiotemporal Labels},
  year = {2019},
  journal={arXiv preprint arXiv:1910.02993},
}
```
