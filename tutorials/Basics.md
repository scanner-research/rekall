# Basics Tutorial
The purpose of this tutorial is to introduce the basics of the Rekall API and
show how Rekall queries, together with the Vgrid visualization interface, can
be used for video analysis.

This is part one of the tutorial, where we download some videos and metadata
for visualization.

## Download Sample Videos and metadata
Download some sample videos and metadata (run from the tutorials folder):
```
wget --no-check-certificate  https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving1.mp4 \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving2.mp4 \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving3.mp4 \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving4.mp4 \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving1.json \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving2.json \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving3.json \
    https://olimar.stanford.edu/hdd/rekall_tutorials/basics/driving4.json
```

## Start a file server for the videos
In a new terminal (from this folder):
```
source activate ...
http-server
```

## Start a jupyter notebook
```
source activate ...
jupyter notebook --ip 0.0.0.0 --port 8000
```
This should open up a new window with the Jupyter environment (if you're
running locally) or give you a link to the Jupyter environment (if you're
running remotely).

## Open the `Basics.ipynb` notebook
The rest of the tutorial continues in the `Basics.ipynb` notebook. Open that
notebook in your Jupyter environment to continue.
