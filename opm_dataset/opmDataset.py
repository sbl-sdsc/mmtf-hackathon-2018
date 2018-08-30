#!/user/bin/env python

import requests
import tempfile
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql import SparkSession
import pandas as pd

def get_dataset():
    ## PARSING CODE HERE!
    url='http://opm.phar.umich.edu/proteins.php'
    opm_html = requests.get(url).content 
    pd_list = pd.read_html(opm_html) 
    opm_df = pd_list[-1]
    opm_df['PDB ID'] = opm_df['PDB ID'].apply(lambda x: x.upper())
    # opm_df.drop(['_c0'])

    # save data to a temporary file (Dataset csv reader requires a input
    # file!)
    tempFileName = _save_temp_file(opm_df)

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


def _save_temp_file(data_frame):
    '''Saves tabular report as a temporary CSV file

    Attributes
    ----------
    data_frame : pandas DataFrame
       pandas DataFrame

    Returns
    -------
    str
       path to the tempfile
    '''
    tempFile = tempfile.NamedTemporaryFile(delete=False)
    data_frame.to_csv(tempFile.name, index=False)
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
