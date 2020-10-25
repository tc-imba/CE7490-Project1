import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


colormap = plt.get_cmap('Dark2')
colors = [colormap(k) for k in np.linspace(0, 1, 8)]
plt.rcParams['axes.prop_cycle'] = plt.cycler(color=colors)

project_root = os.path.dirname(os.path.dirname(__file__))
experiment_dir = os.path.dirname(__file__)
plots_dir = os.path.join(project_root, "plots")
os.makedirs(plots_dir, exist_ok=True)

pd.set_option("display.max_rows", None, "display.max_columns", None, 'display.width', None)

file_format = 'png'
# file_format = 'eps'

PLOT_CONFIG = [
    ("random", "^"),
    ("spar", "X"),
    ("metis", "s"),
    ("online", "o"),
    ("offline", "P"),
]

SMALL_DATASETS = [
    ("facebook", "Facebook"),
    ("arxiv", "Arxiv"),
    ("p2pgnutella", "Gnutella"),
    ("twittersample1", "Twitter I"),
    ("twittersample1", "Twitter II"),
    ("amazonsample", "Amazon I"),
]

LARGE_DATASETS = [
    ("twitter", "Twitter"),
    ("amazon", "Amazon"),
]


def group_by_algorithm(df):
    dfs = []
    for algorithm, marker in PLOT_CONFIG:
        _df = df[df['algorithm'] == algorithm].copy()
        _df = _df.sort_values(by='node', ascending=True)
        dfs.append(_df)
    return dfs


def group_by_dataset(df, dataset, column):
    arr = []
    for algorithm, _ in PLOT_CONFIG:
        temp = []
        for name, _ in dataset:
            _df = df[(df['algorithm'] == algorithm) & (df['data'] == name)]
            _data = _df[column].unique()
            temp.append(_data[0])
        arr.append(temp)
    return arr


def replace_zero_node(df, node):
    for index, row in df.iterrows():
        if row['node'] == 0:
            df.loc[index, 'node'] = node


def plot_facebook_nodes_vs_cost(df, replica):
    filename = 'facebook-nodes-vs-cost-server-%d-replica-%d.%s' % (128, replica, file_format)
    title = 'Facebook, Server = %d, Replica = %d' % (128, replica)
    print(filename, title)

    cond = ((df['data'] == 'facebook') & (df['server'] == 128) & (df['replica'] == replica))
    new_df = df[cond].copy()
    replace_zero_node(new_df, 4039)
    new_dfs = group_by_algorithm(new_df)

    plt.figure()
    for index, _df in enumerate(new_dfs):
        algorithm, marker = PLOT_CONFIG[index]
        plt.plot(_df['node'], _df['cost'], label=algorithm, marker=marker, linestyle="-", linewidth=0.8)

    plt.xlabel('Number of Nodes')
    plt.ylabel('Inter Server Traffic Cost')
    plt.legend()
    plt.title(title)
    plt.xticks([256, 512, 1024, 2048, 4039])
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, filename), dpi=300)
    plt.close()


def plot_dataset_vs_cost_or_time(df, replica, dataset_type, vs_type):
    DATASET = dataset_type == "small" and SMALL_DATASETS or LARGE_DATASETS
    filename = '%s-dataset-vs-%s-server-%d-replica-%d.%s' % (dataset_type, vs_type, 128, replica, file_format)
    title = '%s Dataset, Server = %d, Replica = %d' % (dataset_type.capitalize(), 128, replica)
    print(filename, title)

    cond = ((df['node'] == 0) & (df['server'] == 128) & (df['replica'] == replica))
    new_df = df[cond].copy()

    arr = group_by_dataset(new_df, dataset=DATASET, column=vs_type)
    x = np.arange(len(DATASET))
    width = 0.15

    plt.figure()

    for i, (name, _) in enumerate(PLOT_CONFIG):
        plt.bar(x + width * (i - len(PLOT_CONFIG) / 2. + 0.5), arr[i], width, label=name)

    if vs_type == "cost":
        plt.ylabel('Inter Server Traffic Cost')
    elif vs_type == "time":
        plt.ylabel('Execution Time (s), Log Scale')
        plt.yscale("log")

    plt.legend()
    plt.title(title)
    plt.xticks(x, list(map(lambda x: x[1], DATASET)))
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, filename), dpi=300)
    plt.close()


def main():
    filename = os.path.join(experiment_dir, "result.csv")
    df = pd.read_csv(filename)

    for replica in [0, 2, 3]:
        plot_facebook_nodes_vs_cost(df, replica=replica)

    for vs_type in ["cost", "time"]:
        for replica in [0, 2]:
            for dataset_type in ["small", "large"]:
                plot_dataset_vs_cost_or_time(df, replica=replica, dataset_type=dataset_type, vs_type=vs_type)
        plot_dataset_vs_cost_or_time(df, replica=3, dataset_type="small", vs_type=vs_type)


if __name__ == '__main__':
    main()
