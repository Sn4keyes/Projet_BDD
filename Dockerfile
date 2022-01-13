##### DOCKERFILE FOR IMAGE PYSPARK NOTEBOOK #####
FROM jupyter/pyspark-notebook:python-3.8.8

RUN pip install numpy
RUN pip install pycoingecko
RUN pip install kafka-python
RUN pip install pyspark
RUN pip install pymongo

