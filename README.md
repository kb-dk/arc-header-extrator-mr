ARC Header Extractor for Hadoop
===============================

This is a simple Hadoop tool that extracts the headers from all the records in an [ARC file](http://archive.org/web/researcher/ArcFileFormat.php).

The data is anonymised during the extract.

The tool is Maven based and utilising the [JWAT](https://sbforge.org/display/JWAT/JWAT) library.

A very basic test run, provided your paths and ARC test files matches mine:

    $ mvn package
    $ cd target
    $ tar fzx ARCHeaderExtractor-1.0.tar.gz
    $ cd ARCHeaderExtractor-1.0
    $ export HADOOP_CLASSPATH=/Users/perdalum/Udenfor/github/ARCHeaderExtractorMR/target/ARCHeaderExtractor-1.0/lib/*
    $ hadoop dk.statsbiblioteket.hadoop.archeaderextractor.ARCHeaderExtractorMR /Users/perdalum/Testdata/input-files/ out

More documentation to follow ;-)

