## DASTARD Versions

**0.2.6** September 2019
* Abaco data source uses packets.

**0.2.5** September 5, 2019
* Abaco data source works for streaming data (not packets).
* Reset triggering memory on changing trigger settings.
* Fix `goczmq` API for socket options.
* Fix handling of, and absence of, map files.

**0.2.4** June 17, 2019
* Read Cringe global variable data file.
* Lancero LSYNC stored by Cringe; read by Dastard.
* Refactored WriteControl object.
* Use pointers to `mat.Dense` as recommended by its author.
* Write detector positions in OFF file headers; new OFF version.

**0.2.3** April 19, 2019
* ROACH source works.

**0.2.2** December 19, 2018
* External triggers

**0.2.1** December 7, 2018
* Make mix command accept an array of new mix values and report all back to clients.
* Make SimPulse configuration accept an array of amplitudes to generate multiple pulse sizes.
* Timestamp RPC commands SetExperimentStateLabel when received, not when processed.
* Fix problems with simulated pulse data when read in MASS.
* Get record lengths correct when starting a source.

**0.2.0** October 5, 2018
* Redesign the internals of Dastard to eliminate data races between RPC services
  that try to change data processing and the actual processing. Use more channels.

**0.1.2** September 28, 2018
* Uses an internal buffered channel to queue Lancero data for processing so that
  we can survive 1-second-plus delays in processing (writing seems to be the
  particular problem).

**0.1.1** August 13, 2018
* Demonstrated OFF file writing with triggering.

**0.1.0** July 13, 2018
* Ready for acquiring LJH (and maybe OFF) files with triggering.

**0.0.2** July 2018
* Still improving to reach useful configuration

**0.0.1** July 2018
* Fix irregular data rates for simulated sources due to latencies.


**0.0.0** Development through July 2018.  
* Trying to build a working system.
