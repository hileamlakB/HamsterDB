import utils as ut
import random
import os


server_program = './server'  # relative to running directory
client_program = './client'  # relative to running directory

current_dir = '../profiling'  # relative to running directory
output_dir = '../profiling/results'  # relative to running directory

try:
    os.mkdir('Select')
except OSError as error:
    pass

os.chdir('Select')

# generate test data for different row sized tables
range_size = {
    'small': .05,
    'small1': .10,
    'small': .15,
    'small2': .20,
    'medium': .25,
    'medium2': .50,
    'large': .60,
    'large2': .75,
    'large3': .90,
    'large4': 1,
}

# create a table with uniform distribution for range test
table = ut.Table('test_db', 'selectivity', 3, f'{current_dir}/Select')
table.create_data(1000000)
table.select_query(range_size.values(), timed=True)


selectivity_large = ut.Table(
    'test_db', 'selectivity_large', 3, f'{current_dir}/Select')
selectivity_large.create_data(1000000)


# gerenate test for grouped select
group_size = {
    "group_one": 2,
    "group_two": 4,
    "group_three": 8,
    "group_four": 10,
    "group_five": 16,
    "group_six": 18,
    "group_seven": 20,
    "group_eight": 25,
    "group_nine": 30,
}
load_query = selectivity_large.load_query(save=False)
with open('grouped_select_test.dsl', 'w') as f:

    f.write(load_query)
    for group_name, group_size in group_size.items():
        f.write("timer(start)\n")

        for i in range(group_size):
            f.write(
                f's{i}=select({selectivity_large.db}.{selectivity_large.name}.col0,{ut.MIN_INT},{ut.MAX_INT})\n')
            f.write(
                f'f{i}=fetch({selectivity_large.db}.{selectivity_large.name}.col0,s{i})\n')
        f.write("timer(end)\n")


# generate test for differnt size
tbl_sizes = {
    'very_small': 100,
    'very_small1': 500,
    'small': 1000,
    'small2': 5000,
    'medium': 10000,
    'medium2': 50000,
    'large': 100000,
    'large2': 500000,
    'large3': 1000000,
    'large4': 5000000,
    'very_large': 10000000,
    'very_large2': 50000000,
    'extremely_large1': 100000000,
}
for tbl_name, tbl_size in tbl_sizes.items():
    with open('size_select_test.dsl', 'w') as f:

        selectivity = 0.5
        end = ut.MIN_INT + (ut.MAX_INT - ut.MIN_INT) * selectivity
        for size_name, size in tbl_sizes.items():
            f.write("timer(start)\n")

            f.write(
                f's{tbl_name}=select(test_db.row_test_{tbl_name}.col0,{ut.MIN_INT},{end})\n')
            f.write(
                f'f{tbl_name}=fetch(test_db.row_test_{tbl_name}.col0,s{tbl_name})\n')
            f.write("timer(end)\n")


# test batch operations
batch_sizes = {
    'singlton': 1,
    'small': 2,
    'small1': 4,
    'small2': 8,
    'medium': 16,
    'medium2': 32,
    'medium3': 64,
    'medium4': 128,
    'large': 256,
    'large2': 512,
    'large3': 1024,
    'very_large': 2048,
}


# selectivity of each query is fixed to 0.5 but
# each query reads different sections of a file
with open("batch_select_fixed_selectivity_test.dsl", "w") as f, open("batch_select_random_selectivity_test.dsl", "w") as f2:
    # this will use the large3 table

    selectivity = 0.5
    for batch_name, batch_size in batch_sizes.items():

        f.write("timer(start)\nbatch_queries()\n")
        f2.write("timer(start)\nbatch_queries()\n")
        for i in range(batch_size):
            min_end = ut.get_random_int()
            max_end = max(min_end + (ut.MAX_INT - ut.MIN_INT)
                          * 0.5, ut.MAX_INT)
            max_end2 = max(min_end + (ut.MAX_INT - ut.MIN_INT)
                           * random.random(), ut.MAX_INT)
            f.write(
                f"s{batch_name}{i}=select(test_db.row_test_large3.col2, {min_end}, {max_end})\n")
            f2.write(
                f"s{batch_name}{i}=select(test_db.row_test_large3.col2, {min_end}, {max_end2})\n")
        f.write("batch_execute()\ntimer(end)\n")
        f2.write("batch_execute()\ntimer(end)\n")


# 20 batch queries of fixed selectivity of different file sizes
# batch_size = 20
with open("batch_queries_different_size.dsl", "w") as f:
    selectivity = 0

    f.write("timer(start)\nbatch_queries()\n")
    for file_name, file_size in batch_sizes.items():
        for i in range(batch_size):
            min_end = ut.get_random_int()
            max_end = max(min_end + (ut.MAX_INT - ut.MIN_INT)
                          * 0.5, ut.MAX_INT)
            f.write(
                f"s{file_name}b{i}=select(test_db.row_test_{file_name},{min_end}, {max_end})\n")

    f.write("batch_execute()\ntimer(end)\n")


os.chdir('..')
with open('select_test.sh', 'w') as f:

    # Shell script for range selectivity
    f.write(
        f'echo "Range Selectivity" > {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')

    f.write(
        f'{client_program} < {current_dir}/Select/selectivity.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo range selectivity_test done!!\n')
    f.write(f'{client_program} < ../profiling/shutdown\n')

    # Shell script for group size selectivity
    f.write(
        f'echo "Group Size Selectivity" >> {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')

    f.write(
        f'{client_program} < {current_dir}/Select/grouped_select_test.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo group size selectivity_test done!!\n')
    f.write(f'{client_program} < ../profiling/shutdown\n')

    # Shell script for table size selectivity
    f.write(
        f'echo "Table Size Selectivity" >> {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')
    f.write(
        f'{client_program} < {current_dir}/Select/size_select_test.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo table size selectivity_test done!!\n')

    f.write(f'{client_program} < ../profiling/shutdown\n')

    # shell script for fixed_selectivity batch_select
    f.write(
        f'echo "Fixed selectivity test" >> {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')
    f.write(
        f'{client_program} < {current_dir}/Select/batch_select_fixed_selectivity_test.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo Fixed selectivity test done!!\n')

    f.write(f'{client_program} < ../profiling/shutdown\n')

    # shell script for dynamic selectivty batch_select

    f.write(
        f'echo "Random selectivity test" >> {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')
    f.write(
        f'{client_program} < {current_dir}/Select/batch_select_random_selectivity_test.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo random selectivity test done!!\n')

    f.write(f'{client_program} < ../profiling/shutdown\n')

    # shell script for different table sizes

    f.write(
        f'echo "Different size batch test" >> {output_dir}/select_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/select_server_output.txt &\n')
    f.write(f'sleep 1\n')
    f.write(
        f'{client_program} < {current_dir}/Select/batch_queries_different_size.dsl >> {output_dir}/select_client_output.txt\n')
    f.write(f'echo different size batch test done!!\n')

    f.write(f'{client_program} < ../profiling/shutdown\n')
