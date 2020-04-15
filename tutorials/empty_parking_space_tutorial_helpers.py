from rekall import Interval, IntervalSet, IntervalSetMapping, Bounds3D
from rekall.predicates import *
from vgrid import VGridSpec, VideoMetadata, VideoBlockFormat, FlatFormat, SpatialType_Bbox
from vgrid_jupyter import VGridWidget
import urllib3, requests, os, posixpath
import pickle
from tqdm import tqdm
from PIL import Image
import matplotlib.pyplot as plt

# Hack to disable warnings about olimar's certificate
urllib3.disable_warnings()

# Parking lot data
VIDEO_COLLECTION_BASEURL = "https://storage.googleapis.com/esper/dan_olimar/parking_lot/user_study"
VIDEO_METADATA_FILENAME = 'metadata.json'

# Load video file metadata
video_metadata = [ VideoMetadata(v['filename'], id=v['id'], fps=v['fps'],
                                 num_frames=v['num_frames'], width=v['width'],
                                 height=v['height'])
                  for v in requests.get(posixpath.join(
                      VIDEO_COLLECTION_BASEURL, VIDEO_METADATA_FILENAME),
                                        verify=False).json() ]

VIDEO_FOLDER = 'videos'
BBOX_FOLDER = 'bboxes'
GT_FOLDER = 'empty_spaces'

dev_set = requests.get(
    posixpath.join(VIDEO_COLLECTION_BASEURL, 'dev.txt'), verify=False
).content.decode('utf-8').strip().split('\n')

video_metadata_dev = [
    vm
    for vm in video_metadata if vm.path in dev_set
]

video_metadata = video_metadata_dev

def get_maskrcnn_bboxes():
    interval = 30
    bboxes = [
        pickle.loads(requests.get(
            posixpath.join(
                posixpath.join(VIDEO_COLLECTION_BASEURL, BBOX_FOLDER),
                posixpath.join(vm.path[:-4], 'bboxes.pkl')
            ),
            verify=False
        ).content)
        for vm in (video_metadata)
    ]
    bboxes_ism = IntervalSetMapping({
        metadata.id: IntervalSet([
            Interval(
                Bounds3D(
                    t1 = 30 * i / metadata.fps,
                    t2 = 30 * (i + interval) / metadata.fps,
                    x1 = bbox[0] / metadata.width,
                    x2 = bbox[2] / metadata.width,
                    y1 = bbox[1] / metadata.height,
                    y2 = bbox[3] / metadata.height
                ),
                payload = { 'class': bbox[4], 'score': bbox[5],
                          'spatial_type': SpatialType_Bbox(text=bbox[4])}
            )
            for i, frame in enumerate(bbox_frame_list) if (i % interval == 0)
            for bbox in frame
        ])
        for bbox_frame_list, metadata in tqdm(
            zip(bboxes, (video_metadata)),
            total = len(bboxes))
    })
    
    return bboxes_ism

def get_ground_truth():
    interval = 30
    
    GT_FOLDER = 'empty_spaces'

    empty_parking_spaces = [
        pickle.loads(requests.get(
            posixpath.join(
                posixpath.join(VIDEO_COLLECTION_BASEURL, GT_FOLDER),
                posixpath.join(vm.path[:-4], 'gt.pkl')
            ),
            verify=False
        ).content)
        for vm in video_metadata
    ]
    gt_ism = IntervalSetMapping({
        metadata.id: IntervalSet([
            Interval(
                Bounds3D(
                    t1 = 30 * i / metadata.fps,
                    t2 = 30 * (i + interval) / metadata.fps,
                    x1 = bbox[0] / metadata.width + .01,
                    x2 = bbox[2] / metadata.width - .01,
                    y1 = bbox[1] / metadata.height + .01,
                    y2 = bbox[3] / metadata.height - .01
                )
            )
            for i, frame in enumerate(space_frame_list) if (i % interval == 0)
            for bbox in frame
        ])
        for space_frame_list, metadata in tqdm(
            zip(empty_parking_spaces, video_metadata),
            total = len(empty_parking_spaces))
    })
    
    return gt_ism

def visualize_helper(box_list):
    vgrid_spec = VGridSpec(
        video_meta = video_metadata,
        vis_format = VideoBlockFormat(imaps = [
            (str(i), box)
            for i, box in enumerate(box_list)
        ]),
        video_endpoint = posixpath.join(VIDEO_COLLECTION_BASEURL, VIDEO_FOLDER)
    )
    return VGridWidget(vgrid_spec = vgrid_spec.to_json_compressed())
