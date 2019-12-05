# Rekall Workshop December 6, 2019

Welcome to the Rekall workshop materials! All the material for the workshop
will live in this folder.

Before coming to the workshop, please complete all the materials in the
[workshop prep](#workshop-prep) section (this section may get updated with more
materials as we get closer to the workshop).

## Workshop Prep

Before the workshop, make sure to complete the following steps:

* [Install](#installation) Rekall
* [Load up](#your-first-dataset) your first dataset, and write your first
query (and if you're bringing your own dataset, load up your data)
* [Annotate](#data-annotation) a few ground truth examples of what you want to
query for

### Installation

First, clone this repository in the `workshop` branch:
```bash
git clone -b workshop https://github.com/scanner-research/rekall
```

To run the tutorials and the workshop material, you'll need to install
[Jupyter](https://jupyter.org/install) and [npm](https://www.npmjs.com/).
The easiest way is to install
[anaconda](https://www.anaconda.com/distribution/).

If you have conda installed:
```bash
conda create -y --name rekall python=3.7 anaconda
conda activate rekall
conda install -y -c conda-forge nodejs
conda install -y -c conda-forge nb_conda_kernels

# Install Rekall
pip install rekallpy
npm install --save @wcrichto/rekall

# Install Vgrid visualization tools
npm install --save react react-dom mobx mobx-react
npm install --save @wcrichto/vgrid
pip3 install vgridpy
pip3 install vgrid_jupyter
jupyter nbextension enable --py --sys-prefix vgrid_jupyter
```

If you don't have conda installed, make sure you have Jupyter and NPM
installed, and then run these commands:
```bash
# Install Rekall
pip install rekallpy
npm install --save @wcrichto/rekall

# Install Vgrid visualization tools
npm install --save react react-dom mobx mobx-react
npm install --save @wcrichto/vgrid
pip3 install vgridpy
pip3 install vgrid_jupyter
jupyter nbextension enable --py --sys-prefix vgrid_jupyter
```

### Your First Dataset
Once you've installed Rekall, load up the `Your First Dataset.ipynb` notebook
on your Jupyter server, and walk through it (note that you may have to change
the kernel depending on how you installed Rekall).
This notebook will walk you through loading up a few cable TV news videos, and
writing a first query to find every time Jake Tapper appears.

If you're bringing your own dataset, follow the instructions at the bottom of
the notebook to load up your own dataset.
If you run into any issues, please let us know!

### Data Annotation

Coming soon!
