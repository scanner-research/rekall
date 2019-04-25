# Basics Tutorial

## Download Sample Videos and metadata
Download some sample videos and metadata (run from the tutorials folder):
```
wget...

wget...
```

## Start a file server for the videos
In a new terminal (from this folder):
```
source activate ...
python -m http.server
```

## Start a jupyter notebook
```
source activate ...
jupyter notebook --ip 0.0.0.0 --port 8000
```
This should open up a new window with the Jupyter environment (if you're
running locally) or give you a link to the Jupyter environment (if you're
running remotely).

## Open the `basics.ipynb` notebook
The rest of the tutorial continues in the `basics.ipynb` notebook. Open that
notebook in your Jupyter environment to continue.
