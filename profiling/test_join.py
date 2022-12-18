import utils as ut
import os


server_program = './server'  # relative to running directory
client_program = './client'  # relative to running directory

current_dir = '../profiling'  # relative to running directory
output_dir = '../profiling/results'  # relative to running directory

try:
    os.mkdir('Join')
except OSError as error:
    pass
os.chdir('Join')

# tbl1 = ut.Table('test_db',
#                 'very_large',
#                 3,
#                 f'{current_dir}/Join')
# tbl1.create_data(10000000)
# tbl1.load_query(True, with_db=True, save=True)


# tbl2 = ut.Table('test_db',
#                 'very_large2',
#                 3,
#                 f'{current_dir}/Join')
# tbl2.create_data(50000000)
# tbl2.load_query(True, with_db=False, save=True)


# select joins with different ranges
# hundred % overlap
different_ranges = [.25, .5, .75, .9, .95, 1]


with open("range_join.dsl", "w") as f:

    for i, s in enumerate(different_ranges):
        start = ut.get_random_int()
        end = start + s * (ut.MAX_INT - ut.MIN_INT)

        f.write(f"""p1{i}=select(test_db.very_large2.col1,{start}, {end})\np2{i}=select(test_db.very_large.col1,{start}, {end})\nf1{i}=fetch(test_db.very_large2.col1, p1{i})\nf2{i}=fetch(test_db.very_large.col1, p2{i})\ntimer(start)\nt1{i},t2{i} = join(f1{i},p1{i},f2{i},p2{i})\ntimer(end)\n""")

different_overlaps = [.25, .5, .75, .9, .95, 1]
# different overlaps with selectivity of 0.5

with open("overlap_join.dsl", "w") as f:

    for i, s in enumerate(different_ranges):
        start1 = ut.get_random_int()
        end1 = start + 0.5 * (ut.MAX_INT - ut.MIN_INT)

        start2 = end1 - s * 0.5 * (ut.MAX_INT - ut.MIN_INT)
        end2 = start2 + 0.5 * (ut.MAX_INT - ut.MIN_INT)

        f.write(f"""p1{i}=select(test_db.very_large2.col1,{start},{end})\np2{i}=select(test_db.very_large.col1,{start},{end})\nf1{i}=fetch(test_db.very_large2.col1,p1{i})\nf2{i}=fetch(test_db.very_large.col1,p2{i})\ntimer(start)\nt1{i},t2{i}=join(f1{i},p1{i},f2{i},p2{i},hash)\ntimer(end)\n""")

# different row sizes
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
    # 'exteremly_large': 100000000,
    #   'exteremly_large2': 500000000,
}

# selectivity of 0.5
first = True
with open("row_join.dsl", "w") as f, open("row_join_nested.sh", "w") as f2:
    for tbl_name, tbl_size in tbl_sizes.items():
        T1 = ut.Table('test_db', 'row_test_' + tbl_name, 3,
                      f'{current_dir}/Join')
        # T1.create_data(tbl_size)
        # T1.load_query(True, with_db=False, save=True)
        # if first:
        #     first = False
        start = ut.get_random_int()
        end = start + 0.5 * (ut.MAX_INT - ut.MIN_INT)
        f.write(f"""p1{tbl_name}=select(test_db.row_test_{tbl_name}.col1,{start}, {end})\np2{tbl_name}=select(test_db.row_test_very_large.col1,{start}, {end})\nf1{tbl_name}=fetch(test_db.row_test_{tbl_name}.col1, p1{tbl_name})\nf2{tbl_name}=fetch(test_db.row_test_very_large.col1, p2{tbl_name})\ntimer(start)\nt1{tbl_name},t2{tbl_name}=join(f1{tbl_name},p1{tbl_name},f2{tbl_name},p2{tbl_name},hash)\ntimer(end)\n""")
        f2.write(f"""p1{tbl_name}=select(test_db.row_test_{tbl_name}.col1,{start}, {end})\np2{tbl_name}=select(test_db.row_test_very_large.col1,{start}, {end})\nf1{tbl_name}=fetch(test_db.row_test_{tbl_name}.col1, p1{tbl_name})\nf2{tbl_name}=fetch(test_db.row_test_very_large.col1, p2{tbl_name})\ntimer(start)\nt1{tbl_name},t2{tbl_name}=join(f1{tbl_name},p1{tbl_name},f2{tbl_name},p2{tbl_name},nested-loop)\ntimer(end)\n""")
os.chdir('..')

# create test files
with open("join_test.sh", 'w') as f:
    f.write(f"""echo "Join test ranges" > {output_dir}/join_server_out.txt\n{server_program} >> {output_dir}/join_server_out.txt &\nsleep 1\n{client_program} < {current_dir}/Join/range_join.dsl >> {output_dir}/join_client_output.txt\n{client_program} < {current_dir}/shutdown\necho "Join test Done!!"

            echo "Join test overlap" >> {output_dir}/join_server_out.txt
            {server_program} >> {output_dir}/join_server_out.txt &\nsleep 1\n{client_program} < {current_dir}/Join/overlap_join.dsl >> {output_dir}/join_client_output.txt\n{client_program} < {current_dir}/shutdown\necho "Join test overlap!!"

            echo "Join test row" > {output_dir}/join_server_out.txt
            {server_program} >> {output_dir}/join_server_out.txt &\nsleep 1\n{client_program} < {current_dir}/Join/row_join.dsl >> {output_dir}/join_client_output.txt\n{client_program} < {current_dir}/shutdown\necho "Join test row Done!!"
            """)
