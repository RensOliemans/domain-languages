# Domain languages

Detect languages used in different top-level domain websites.


## Packages
For get-warc we use

    selectolax langdetect cdx_toolkit venv-pack

For simple-nlp we use

    spark-nlp==2.6.5 numpy venv-pack


## Roadmap
* ~Merge simple-nlp and get-warc~
* Load cdxtoolkit in spark
* Do langdetect with sparknlp, distributed
* Output results
