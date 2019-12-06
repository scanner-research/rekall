# Rekall Workshop December 6, 2019

Welcome to the Rekall workshop materials! All the material for the workshop
will live in this folder.

Before coming to the workshop, please complete all the materials in the
[workshop prep](#workshop-prep) section (this section may get updated with more
materials as we get closer to the workshop).

During (or after) the workshop, check out the
[workshop materials](#workshop-materials) section.
We'll walk through the notebooks throughout the course of the afternoon!

## Workshop Prep

Before the workshop, make sure to complete the following steps:

* [Install](#installation) Rekall
* [Load up](#your-first-dataset) your first dataset, and write your first
query (and if you're bringing your own dataset, load up your data)
<!--
* [Annotate](#data-annotation) a few ground truth examples of what you want to
query for
-->

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
pip install vgridpy
pip install vgrid_jupyter
jupyter nbextension enable --py --sys-prefix vgrid_jupyter
```

If you don't have conda installed, make sure you have Jupyter and NPM
installed, and then run these commands (note that our Python packages are
Python 3.5+ only):
```bash
# Install Rekall
pip3 install rekallpy
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

<!--
### Data Annotation

Coming soon!
-->


## Workshop Materials

### Your First Rekall Queries

First, we'll walk you through developing the Bernie Sanders interview query that
appears in the [Rekall paper](https://arxiv.org/abs/1910.02993).
Get started with `Bernie Sanders Interviews.ipynb`.

Next, we'll have you walk through an empty parking space detection query.
Check out the `Empty Parking Space Detection.ipynb`.
We'll walk through that notebook once everyone's had a go at it as well.

### Hands-On Time

Next comes the hands-on time - the purpose of this section of the day is to get
everyone writing new queries about things that you care about.
If you brought your own dataset, take the skills that you learned from the last
two notebooks, and apply them to some of the queries that you're interested in.

If you didn't bring your own dataset, check out
`Hands-On Time - TV News Data.ipynb`.
That notebook loads up some TV News data for you and gives you a few example
queries to try:
* Find commercials for drugs
* Find political commercials
* Find commercials about phones or carriers (Verizon, Sprint, etc)
* Find instances of panels (multiple people brought on to talk about one subject)
* Find segments about guns

### Auto-Tuning Your Rekall Queries for Better Performance

Once you've spent some time writing queries, we'll introduce the Rekall tuning
modules.
Check out `The Rekall Auto-Tuner.ipynb` to get an introduction to the tuning API.

The Rekall tuning module requires some ground truth annotations - if you came to
the workshop with some ground truth annotations for your queries, you can use
these to tune the queries you wrote during the hands-on session now.
Otherwise, check out `Annotating Ground Truth.ipynb` to see how you can use
Vgrid to annotate some ground-truth temporal segments.