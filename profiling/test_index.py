import utils as ut
import os


server_program = './server'  # relative to running directory
client_program = './client'  # relative to running directory

current_dir = '../profiling'  # relative to running directory
output_dir = '../profiling/results'  # relative to running directory

try:
    os.mkdir('Index')
except OSError as error:
    pass

os.chdir('Index')

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
    'exteremly_large': 100000000,
    # 'exteremly_large2': 500000000,
}
first = True

with open("clustered_select.dsl", "w") as f, open("unclustered_select.dsl", "w") as f2:
    for tbl_name, tbl_size in tbl_sizes.items():
        T1 = ut.Table('test_db', 'row_test_' + tbl_name, 3,
                      f'{current_dir}/Index')
        T1.create_data(tbl_size)
        lst = [('col2', 'sorted', 'clustered'),
               ('col3', 'btree', 'unclustered')]
        T1.index_query(timed=True, with_db=first, save=True, lst_of_idxs=lst)
        if first:
            first = False

        start = ut.get_random_int()
        end = start + 0.5 * (ut.MAX_INT - ut.MIN_INT)

        f.write(
            f"timer(start)\ns{tbl_name}=select(test_db.row_test_{tbl_name}.col2, {start}, {end});\ntimer(end)\n"
        )

        f2.write(
            f"timer(start)\ns{tbl_name}=select(test_db.row_test_{tbl_name}.col3, {start}, {end});\ntimer(end)\n"
        )


os.chdir('..')
# create test files
with open("index_test.sh", 'w') as f:
    f.write(
        f"""echo "Index load test" > {output_dir}/index_server_out.txt\n{server_program} >> {output_dir}/index_server_out.txt &\nsleep 1\n""")

    for tbl_name, tbl_size in tbl_sizes.items():
        f.write(
            f"""echo "Index load test {tbl_name}" >> {output_dir}/index_server_out.txt\n""")
        f.write(
            f"""{client_program} < {current_dir}/Index/row_test_{tbl_name}.dsl >> {output_dir}/index_client_output.txt\n""")
        f.write(f"""{client_program} < {current_dir}/shutdown\n""")
        f.write(f"echo loading {tbl_name} done\n")
        f.write(f""" {server_program} >> {output_dir}/index_server_out.txt &\n
            sleep 1\n""")
