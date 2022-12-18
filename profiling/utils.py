import string
import random
import numpy as np
import time
import pandas as pd
import os

working_dir = '../profiling'  # relative to running directory, this should later
# bechanged to argc argv
os.chdir(working_dir)


MIN_INT = -2147483647 - 1
MAX_INT = 2147483647


def get_random_string(length):

    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def get_random_array(length):

    np.random.seed(int(time.time()))
    return np.random.randint(MIN_INT, MAX_INT, length)


def get_random_int():
    np.random.seed(int(time.time()))
    return random.randint(MIN_INT, MAX_INT)


class Table:
    def __init__(self, db_name, name, num_cols, odir):
        self.db = db_name
        self.name = name
        self.col_names = ['col' + str(i) for i in range(num_cols)]
        self.data = {f'{self.db}.{self.name}.{col}': []
                     for col in self.col_names}
        self.file_name = name
        self.dir = odir

    def load_query(self, timed=False, with_db=False, save=True):
        """
            creates a load query for the table
        """
        query = ""
        if with_db:
            query += f'create(db, "{self.db}")\n'
        query += f'create(tbl,"{self.name}",{self.db},{len(self.col_names)})\n'
        for col in self.col_names:
            query += f'create(col,"{col}",{self.db}.{self.name})\n'

        if timed:
            query += 'timer(start)\n'

        query += f'load("{self.dir}/{self.file_name}.csv")\n'

        if timed:
            query += 'timer(end)\n'

        if save:
            with open(self.file_name+'.dsl', 'w') as f:
                f.write(query)
        return query

    def index_query(self, lst_of_idxs=[], timed=False, with_db=False, save=True):
        """
            creates an index query for the table
        """

        query = self.load_query(timed=False, with_db=with_db, save=False)
        for col, idx_type, cluster_type in lst_of_idxs:
            if timed:
                query += 'timer(start)\n'

            query += f'create(idx,{self.db}.{self.name}.{col},{idx_type},{cluster_type})\n'

            if timed:
                query += 'timer(end)\n'
        if save:
            with open(self.file_name+'.dsl', 'w') as f:
                f.write(query)

    def select_query(self, selectivity_ranges, timed=False, save=True, load=True, group_timer=False):
        """
            creates a select of different selectivity ranges
        """

        if load:
            # first load the table for testing
            query = self.load_query(timed=False, with_db=False, save=False)

        if group_timer:
            query += 'timer(start)\n'

        for i, rng in enumerate(selectivity_ranges):
            num1 = MIN_INT
            num2 = num1 + rng * (MAX_INT - MIN_INT)

            if (num2 > MAX_INT):
                num2 = num1 - rng * (MAX_INT - MIN_INT)

            if timed and not group_timer:
                query += 'timer(start)\n'

            query += f's{i}=select({self.db}.{self.name}.{random.choice(self.col_names)},{min(num1, num2)},{max(num1, num2)})\n'
            query += f'f{i}=fetch({self.db}.{self.name}.{random.choice(self.col_names)},s{i})\n'

            if timed and not group_timer:
                query += 'timer(end)\n'

        if group_timer:
            query += 'timer(end)\n'

        if save:
            with open(self.file_name+'.dsl', 'w') as f:
                f.write(query)
        return query

    def create_data(self, num_rows):
        for col in self.data:
            self.data[col] = get_random_array(num_rows)

        table_file = pd.DataFrame(self.data)
        table_file.to_csv(f'{self.file_name}.csv', index=False)
