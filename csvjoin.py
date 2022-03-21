#!/usr/bin/env python3

"""This is a CLI program designed for joining csv tables.
To see instructions how program works simply type "python3 join.py --help"
"""

import pandas as pd
import argparse
import sys

# Argprase initialization
parser = argparse.ArgumentParser(prog='csvjoin',
                                description='Join two csv tables using specified column. Output is also in csv format.',
                                epilog='Enjoy joyning!')
parser.add_argument('file_path_1',
                    help='The path to the left csv table')
parser.add_argument('file_path_2',
                    help='The path to the right csv table')
parser.add_argument('column_name',
                    help='Name of the column to join by')
parser.add_argument('join_type',
                    help='Type of performed join [inner, left, right, outer, cross]')
parser.add_argument('-c',
                    '--chunksize',
                    help='Size of chunks (in rows) in whitch data are loaded to RAM. Note that cross join may '
                    'require smaller chunksize')
args = parser.parse_args()

# Pandas function responsible for the main joining task
def join_two(path_1, path_2, column_name, join_type, chunk_size=1000):
    """Load and join two tables in csv format, using one specified column and join type.
        args:
            path_1 - path to the left csv table
            path_2 - path to the right csv table
            column_name - name of the column in BOTH tables by which we connect
            join_type - one of the join types [inner, left, right, outer, cross]
            chunk_size - size of chunks in whitch data are loaded to RAM
        return:
            pandas.DataFrame object containing joined tables
    """
    df1 = pd.read_csv(path_1, chunksize=chunk_size)
    df2 = pd.read_csv(path_2, chunksize=chunk_size)
    df_joined = pd.DataFrame()

    # Cross join does not require column name so is set to None
    if join_type == 'cross':
        column_name = None
    
    for df1_chunk in df1:
        for df2_chunk in df2:
            chunks_merged = pd.merge(df1_chunk, df2_chunk, on=column_name, how=join_type)
            df_joined = df_joined.append(chunks_merged)
    return df_joined

# Optional argument handling
if args.chunksize == None: 
    chunk_size = 1000   
else:   
    chunk_size = args.chunksize

# Program execution and error handling. 
if args.join_type not in ('inner', 'left', 'right', 'outer', 'cross'):
    print(f"csvjoin: '{args.join_type}': Wrong join type. Should be one of [inner, left, right, outer, cross]", file=sys.stderr)
else:
    try:
        print(join_two(args.file_path_1, 
                    args.file_path_2, 
                    args.column_name, 
                    args.join_type,
                    chunk_size).to_csv())
    except KeyError:
        print(f"csvjoin: '{args.column_name}': Something is wrong with column name. Make sure that it is correct and present in both tables.", file=sys.stderr)
    except FileNotFoundError:
        print(f"csvjoin: '{args.file_path_1}, {args.file_path_2}': Couldn't locate given files.", file=sys.stderr)
