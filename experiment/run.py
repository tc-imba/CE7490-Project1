import asyncio
import os
import subprocess
import functools

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
program = os.path.join(project_root, "cmake-build-release", "social_network")
data_root = os.path.join(project_root, "data")
result_dir = os.path.join(project_root, "result")
os.makedirs(result_dir, exist_ok=True)

workers = 20
count = 0

TIMEOUT = 36000

ALGORITHMS = ["random", "spar", "metis", "online", "offline"]
DATASETS_SMALL = ["facebook", "arxiv", "p2pgnutella", "amazonsample", "twittersample1", "twittersample2"]
DATASETS_LARGE = ["amazon", "twitter"]


async def run_program(data: str, algorithm: str, server: int, replica: int, node: int = 0):
    data_filename = os.path.join(data_root, data + ".txt")
    base_filename = "%s-%s-%d-%d-%d" % (data, algorithm, server, replica, node)
    output_filename = os.path.join(result_dir, base_filename)
    args = [
        program,
        "-d", data_filename,
        "-a", algorithm,
        "-s", str(server),
        "-k", str(replica),
        "-n", str(node),
    ]

    global workers, count
    while workers <= 0:
        await asyncio.sleep(1)

    workers -= 1
    p = None
    try:
        # print(args)
        p = await asyncio.create_subprocess_exec(
            program, *args,
            stderr=subprocess.PIPE,
            stdout=open(output_filename, 'w')
        )
        await asyncio.wait_for(p.communicate(), timeout=TIMEOUT)
    except asyncio.TimeoutError:
        print('timeout!')
    except Exception as e:
        pass
        # print(e)
    try:
        p.kill()
    except Exception as e:
        pass
        # print(e)
    workers += 1
    count += 1
    print('%s (%d)' % (output_filename, count))


async def run_facebook():
    dataset = "facebook"
    tasks = []
    for node in [256, 512, 1024, 2048]:
        for algorithm in ALGORITHMS:
            # tasks.append(run_program(dataset, algorithm, 128, 0, node))
            tasks.append(run_program(dataset, algorithm, 128, 2, node))
            # tasks.append(run_program(dataset, algorithm, 128, 3, node))
    await asyncio.gather(*tasks)


async def run_small():
    tasks = []
    for dataset in DATASETS_SMALL:
        for algorithm in ALGORITHMS:
            tasks.append(run_program(dataset, algorithm, 128, 0))
            tasks.append(run_program(dataset, algorithm, 128, 2))
            tasks.append(run_program(dataset, algorithm, 128, 3))
    await asyncio.gather(*tasks)


async def run_large():
    tasks = []
    for dataset in DATASETS_LARGE:
        for algorithm in ALGORITHMS:
            tasks.append(run_program(dataset, algorithm, 128, 0))
            tasks.append(run_program(dataset, algorithm, 128, 2))
    await asyncio.gather(*tasks)


async def main():
    tasks = [run_facebook()]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
