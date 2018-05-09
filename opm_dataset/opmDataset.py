#!/user/bin/env python

import requests
import tempfile
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql import SparkSession

def get_dataset():
    ## PARSING CODE HERE!


    # save data to a temporary file (Dataset csv reader requires a input
    # file!)
    tempFileName = _save_temp_file(unzipped)

    #load temporary CSV file to Spark dataset
    dataset = _read_csv(tempFileName)
    dataset = _remove_spaces_from_column_names(dataset)

    return dataset


def _decode_as_zip_input_stream(content):
    '''Returns an input stream to the first zip file entry

    Attributes
    ----------
    content : inputStream
       inputStream content from request

    Returns
    -------
    inputStream
       unzipped InputStream
    '''

    zipfile = ZipFile(BytesIO(content))
    return [line.decode() for line \
            in zipfile.open(zipfile.namelist()[0]).readlines()]


def _save_temp_file(unzipped):
    '''Saves tabular report as a temporary CSV file

    Attributes
    ----------
    unzipped : list
       list of unzipped content

    Returns
    -------
    str
       path to the tempfile
    '''
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    with open (tempFile.name, "w") as t:
        t.writelines(unzipped)
    return tempFile.name


def _read_csv(inputFileName):
    '''Reads CSV file into Spark dataset

    Attributes
    ----------
    fileName : str
       name of the input csv fileName

    Returns
    -------
    dataset
       a spark dataset
    '''
    spark = SparkSession.builder.getOrCreate()
    dataset = spark.read.format("csv") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(inputFileName)
    return dataset


def _remove_spaces_from_column_names(original):
    '''Remove spaces from column names to ensure compatibility with parquet

    Attributes
    ----------
    original : dataset
       the original dataset

    Returns
    -------
    dataset
       dataset with columns renamed
    '''

    for existingName in original.columns:
        newName = existingName.replace(' ','')
        # TODO: double check dataset "withColumnRenamed" funciton
        original = original.withColumnRenamed(existingName,newName)

    return original