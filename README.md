# rekall [![Build Status](https://travis-ci.org/scanner-research/rekall.svg?branch=master)](https://travis-ci.org/scanner-research/rekall)
Rekall is a spatiotemporal query language. It operates over sets of intervals and allows for combining and filtering on temporal and spatial predicates.

Rekall has a primary [Python API](https://github.com/scanner-research/rekall/tree/master/rekallpy) for all the core interval processing operations. Rekall also has a [Javascript API](https://github.com/scanner-research/rekall/tree/master/rekalljs) which we use for the [vgrid](https://github.com/scanner-research/vgrid) video metadata visualization widget.

## Installation

### Python API

#### Through pip

```
pip3 install rekallpy
```

#### From source

```
git clone https://github.com/scanner-research/rekall
cd rekall/rekallpy
pip3 install .
```

### Javascript API

#### Through npm

```
npm install --save @wcrichto/rekall
```

#### From source

```
git clone https://github.com/scanner-research/rekall
cd rekall/rekalljs
npm install
npm run prepublishOnly
npm link
```
