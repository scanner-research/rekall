# Rekall Tutorials

We provide a few tutorials to get you started with Rekall.
We recommend starting with the Basics Tutorial.
This will walk you through Rekall's basic API and show you how to visualize
Rekall queries with Vgrid.

To run these tutorials, you'll need to install `vgrid` and `vgrid_jupyter` for
visualization.

You'll need `npm` installed, as well as Python3.5 or greater.

### VGrid:
```
npm install --save react react-dom mobx mobx-react
npm install --save @wcrichto/vgrid
pip3 install vgridpy
```

Or [from source](https://github.com/scanner-research/vgrid/blob/master/DEVELOPMENT.md).

### VGrid Jupyter Plugin:
```
pip3 install vgrid_jupyter
jupyter nbextension enable --py --sys-prefix vgrid_jupyter
```

Or [from source](https://github.com/scanner-research/vgrid_jupyter#from-source).

## Basics
Start with `Basics.ipynb`. This will walk you through visualizing bounding
boxes on some videos from BDD.
