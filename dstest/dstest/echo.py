from __future__ import absolute_import, division, print_function
import pyarrow.parquet as pq
import logging
import os
import pandas as pd
import argparse

def echo_opts():
    parser = argparse.ArgumentParser()

    # Required parameters
    parser.add_argument("--input_data_path",
                        default=None,
                        type=str,
                        required=True,
                        help="The input data path. Should be the .tsv files (or other data files) for the task.")
    parser.add_argument("--out_data_path",
                        default=None,
                        type=str,
                        required=True,
                        help="The out data path. Should be the .tsv files (or other data files) for the task.")
    
    # Other parameters
    parser.add_argument("--column",
                        type=str,
                        default='Column',
                        help="column name to append"
                        )
    return parser


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logging.info(f"in dstest echo")
logging.info(f"Load pyarrow.parquet explicitly: {pq}")
logger = logging.getLogger(__name__)

class EchoProcessor(object):
  """Echo"""

  def parse_param(self, meta: dict = None):
    if meta:
      self.column_name = meta.get("Column", "NotConfigured")

  def __init__(self, meta: dict = None):
    self.column_name = "Default"
    self.parse_param(meta)

  def run(self, df, meta: dict = None):
    self.parse_param(meta)
    col = df[df.columns[0]]
    df.insert(len(df.columns), self.column_name, col.values, True)
    return df


def read_parquet(data_path):
    """

    :param file_name: str,
    :return: pandas.DataFrame
    """
    logger.info("start reading parquet.")
    df = pd.read_parquet(os.path.join(data_path, 'data.dataset.parquet'), engine='pyarrow')
    logger.info("parquet read completed.")
    return df

def main():
  parser = echo_opts()
  args = parser.parse_args()
  try:
    df = read_parquet(args.input_data_path)
  except:
    data = {'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'], 
      'Height': [5.1, 6.2, 5.1, 5.2], 
      'Qualification': ['Msc', 'MA', 'Msc', 'Msc']} 
    df = pd.DataFrame(data)
  print(df)

  # Define a dictionary containing Students data 
  meta = {
    "Column": args.column
  }

  processor = EchoProcessor(meta)
  df1 = processor.run(df)
  print(df1)

  df.to_parquet(fname=os.path.join(args.out_data_path, "feature.parquet"), engine='pyarrow')

#python echo.py --input_data_path ../input --out_data_path ../output --column NewColumn
if __name__ == "__main__":
  main()
