# Rekall Tutorials

We provide a few tutorials to get you started with Rekall.
We recommend starting with the Cyclist Detection Tutorial.
This will walk you through Rekall's basic API and show you how to visualize
Rekall queries with Vgrid.

To run these tutorials, you'll need to install `vgrid` and `vgrid_jupyter` for
visualization.
You should also clone the Rekall repository to get the tutorial notebooks:
```
git clone https://github.com/scanner-research/rekall
```

### VGrid:
You'll need Python3.5 or greater.
```
pip3 install vgridpy
```

Or [from source](https://github.com/scanner-research/vgrid/blob/master/DEVELOPMENT.md).

### VGrid Jupyter Plugin:
```
pip3 install vgrid_jupyter
jupyter-nbextension enable --py --sys-prefix vgrid_jupyter
```

Or [from source](https://github.com/scanner-research/vgrid_jupyter#from-source).

## Cyclist Detection
Start with `01 Cyclist Detection.ipynb`.
This will walk you through a simple example using person and bike detections to
detect a new class (bicyclists) using Rekall's operations.

## Empty Parking Space Detection
Next we recommend moving on to `02 Empty Parking Space Detection.ipynb`.
This tutorial will walk you through detecting empty parking spaces in a
static-camera feed of a parking lot using nothing more than the outputs of an
off-the-shelf object detector.

## Data Loading and Visualization
We recommend `03 Data Loading and Visualization.ipynb` after completing
the other two tutorials.
This will walk you through how to load and visualize your own data using Rekall.

## The Rekall Auto-tuner
`04 The Rekall Auto-Tuner.ipynb` will teach you how to use Rekall's auto-tuner to
automatically tune the parameters of a query (the "magic numbers").
