import os
import subprocess

from colorama import Fore, init

init(autoreset=True)

dir = "bench_0930"

if not os.path.exists(dir):
    os.makedirs(dir)


def run_shell(cmd):
    """
    Executes a shell command and returns the output, error message, and exit status.

    Parameters:
    - cmd (str): The shell command to run.

    Returns:
    - output (str): The stdout from running the command.
    - error (str): The stderr from running the command.
    - exit_status (int): The exit status of the command. Zero indicates success.
    """
    try:
        # Execute the command
        result = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True, preexec_fn=os.setsid)
        # If the command was successful, return the output and empty error with exit status
        return result.stdout, "", result.returncode
    except subprocess.CalledProcessError as e:
        # If the command failed, return the error output and exit status
        return "", e.stderr, e.returncode


# Example usage
if __name__ == "__main__":
    commands = [
        # "git switch compaction",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # f"--threads=1 --profile 2>> {dir}/tpc-h_profile_no_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # f"--threads=1 --profile 2>> {dir}/tpc-ds_profile_no_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # f"--threads=1 --profile 2>> {dir}/imdb_profile_no_cpt.log ",

        # "git switch logical_compaction",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # f"--threads=1 2>> {dir}/imdb_profile_logical_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # f"--threads=1 2>> {dir}/tpc-ds_profile_logical_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # f"--threads=1 2>> {dir}/tpc-h_profile_logical_cpt.log ",

        # "git switch no_cpt",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # f"--threads=1 2>> {dir}/tpc-ds_profile_no_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # f"--threads=1 2>> {dir}/imdb_profile_no_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # f"--threads=1 2>> {dir}/tpc-h_profile_no_cpt.log ",
        #
        # "git switch full_cpt",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # "--threads=1 --profile 2>> {dir}/imdb_full_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-ds_full_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-h_full_cpt.log ",
        #
        # "git switch binary_cpt",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # "--threads=1 --profile 2>> {dir}/imdb_binary_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-ds_binary_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-h_binary_cpt.log ",
        #
        # "git switch dynamic_cpt",
        # "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        # "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        # "--threads=1 --profile 2>> {dir}/imdb_dynamic_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-ds_dynamic_cpt.log ",
        # "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        # "--threads=1 --profile 2>> {dir}/tpc-h_dynamic_cpt.log ",

        # "git switch logical_cpt",
        "BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make",
        "build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' "
        f"--threads=1 2>> {dir}/pure_logical_cpt_v1.log ",
        "build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)' "
        f"--threads=1 2>> {dir}/pure_logical_cpt_v1.log ",
        "build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)' "
        f"--threads=1 2>> {dir}/pure_logical_cpt_v1.log ",
    ]

    for cmd in commands:
        output, error, status = run_shell(cmd)

        if output:
            output_lines = output.strip().split('\n')
            if len(output_lines) > 100:
                print('\n'.join(output_lines[:10] + ['...'] + output_lines[-10:]), end='\n')
            else:
                print(output, end='')

        if error:
            error_lines = error.strip().split('\n')
            if len(error_lines) > 100:
                print(Fore.RED + '\n'.join(error_lines[:10] + ['...'] + error_lines[-10:]), end='\n')
            else:
                print(Fore.RED + error, end='')

            exit(0)
