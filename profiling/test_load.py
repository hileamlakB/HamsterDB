import utils as ut
import os


server_program = './server'  # relative to running directory
client_program = './client'  # relative to running directory

current_dir = '../profiling'  # relative to running directory
output_dir = '../profiling/results'  # relative to running directory

try:
    os.mkdir('Load')
except OSError as error:
    pass

os.chdir('Load')

# generate test data for different row sized tables
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
}
first = True
for tbl_name, tbl_size in tbl_sizes.items():
    T1 = ut.Table('test_db', 'row_test_' + tbl_name, 5,
                  f'{current_dir}/Load')
    T1.create_data(tbl_size)
    T1.load_query(True, first)
    if first:
        first = False

# generate test data for different column sized tables
col_sizes = {
    'one': 1,
    'three': 3,
    'five': 5,
    'ten': 10,
    'fifteen': 15,
    'twenty': 20,
}
for col_name, col_size in col_sizes.items():
    T2 = ut.Table('test_db', 'col_test_' + col_name, col_size,
                  f'{current_dir}/Load')
    T2.create_data(1000000)
    T2.load_query(True, False)

os.chdir('..')


with open('load_test.sh', 'w') as f:
    f.write(f'echo "ROW LOAD TEST" > {output_dir}/load_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/load_server_output.txt &\n')
    f.write(f'sleep 1\n')

    for tbl_name in tbl_sizes:
        f.write(
            f'{client_program} < {current_dir}/Load/row_test_{tbl_name}.dsl >> {output_dir}/load_client_output.txt\n')
        f.write(f'echo row_test_{tbl_name} done!!\n')

    f.write(f'{client_program} < {current_dir}/shutdown\n')

    f.write(f'echo "COL LOAD TEST" >> {output_dir}/load_server_output.txt\n')
    f.write(f'{server_program} >> {output_dir}/load_server_output.txt &\n')
    f.write(f'sleep 1\n')

    for col_name in col_sizes:
        f.write(
            f'{client_program} < {current_dir}/Load/col_test_{col_name}.dsl >> {output_dir}/load_client_output.txt\n')
        f.write(f'echo col_test_{col_name} done!!\n')

    f.write(f'{client_program} < {current_dir}/shutdown\n')
