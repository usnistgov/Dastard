# Dastard
[![Build Status](https://travis-ci.org/usnistgov/dastard.svg?branch=master)](https://travis-ci.org/usnistgov/dastard)

A data acquisition framework for NIST transition-edge sensor (TES) microcalorimeters. Designed to replace the earlier programs `ndfb_server` and `matter` (see their [bitbucket repository](https://bitbucket.org/nist_microcal/nasa_daq)).

## Installation
 * Requires Ubuntu 16.

 ```
 sudo add-apt-repository 'deb http://download.opensuse.org/repositories/network:/messaging:/zeromq:/git-stable/xUbuntu_16.04/ ./'
cd ~/Downloads
wget http://download.opensuse.org/repositories/network:/messaging:/zeromq:/git-stable/xUbuntu_16.04/Release.key
sudo apt-key add - < Release.key
sudo apt-get update
sudo apt-get install -y libsodium-dev python-pyqt5 libczmq-dev roxterm git golang-1.10-go golang-1.10
sudo ln -s /usr/lib/go-1.10/bin/go /usr/local/bin/go
go get -u github.com/usnistgov/dastard
```
 * Install microscope https://github.com/usnistgov/microscope
 * Install dastard-commander https://github.com/usnistgov/dastard-commander

#### On MacOS you can get it to run with macports, but you will need
 * `sudo cp /opt/local/lib/pkgconfig/libzmq.pc /opt/local/lib/pkgconfig/zmq.pc` to work around a bug in macports
 
## Purpose

DASTARD is the Data Acquisition System for Triggering, Analyzing, and Recording Data. It is to be used with TES microcalorimeter arrays and the NIST Time-Division Multiplexed (TDM) readout or--in the future--the Microwave Multiplexed readout.

Assuming that the TDM readout system is properly configured first (beyond the scope of this project), one can use Dastard to:
1. read the high-rate data stream;
1. search it for x-ray pulse triggers; generate "triggered records" of a fixed length;
1. store these records to disk in the LJH data format;
1. "publish" the full records to a socket for other programs to plot or use as they see fit;
1. compute important derived quantities from these records.

### Goals, or, why did we need Dastard?

Some reasons to replace the earlier data acquisition programs with Dastard in Go:

1. To permit simple operation of more than one data source of the same type under one program (e.g., multiple uMUX boxes or multiple PCI-express interfaces to the TDM crate). With previous DAQ programs, multiple PCI-e cards required multiple invocations of the data server, which were difficult to coordinate.
1. To employ multi-threading to parallelize over multiple cores, and do so in a language with natural concurrency constructs. (This was attempted in Matter, but it never worked because of the algorithm used there to generate group triggering.)
1. To separate the GUI from the "back-end" code. This keeps the core functions from having to share time with the GUI event loop, and it allows for more natural use of "external commanding", such as we require at synchrotron beamlines.
1. To break the rigid assumption that channels fall into error/feedback pairs. For microwave MUX systems, this will be quite useful.
1. To use code testing from the outset and ensure code correctness.
1. To start with a clean slate, so that we can more easily add new features, such as
   * Generalize the group trigger (each channel can receive triggers from its own list of sources). This will help with cross-talk studies and with the Kaon project HEATES.
   * Allow writing of group-triggered ("secondary") pulses to different files.
   * Generate "variable-length records" for certain high-rate analyses. (This is still hypothetical.)

Some goals for DASTARD, in addition to these reasons:

* Scale up to operate even with future, larger arrays.
* No longer require C++ expertise to work on DAQ software, and to use only Python for the GUI portions.

## Project structure

DASTARD is a back-end program written in Go. It handles the "server" aspects of `ndfb_server`, as well as the triggering and data-recording duties of `matter`. It is complemented by two GUI-based projects: a control GUI and a data plotter.

* [dastard-commander](https://github.com/usnistgov/dastard-commander) is a Python-Qt5 GUI to control Dastard.
* [microscope](https://github.com/usnistgov/microscope) is a Qt-based plotting GUI to plot microcalorimeter pulse records, discrete Fourier transforms, and related data. It is written in C++ and contains code from [matter](https://bitbucket.org/nist_microcal/nasa_daq/)

We envision future control clients other than dastard-commander. They should enable commanding from, for example, the beamline control system of a synchrotron.


### Contributors and acknowledgments

* Joe Fowler (@joefowler or joe.fowler@nist). Sept 2017-present
* Galen O'Neil (@ggggggggg or oneilg@nist). May 2018-present

For all email address, replace "@nist" with "@nist.gov".

Many key concepts in Dastard were adapted from the programs `ndfb_server`, `xcaldaq_client`, and `matter`, written by Dastard's authors and by contributors from both NIST Boulder Laboratories and NASA Goddard Space Flight Center.
