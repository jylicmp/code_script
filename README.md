# code_script

一些用于 HPC 计算的实用脚本。目前主要包含 `WannierBerri/ray_Qiming`：一套在 LSF 集群上用 Ray 并行运行 WannierBerri，并记录 Ray worker 内存占用的模板脚本。

## 目录结构

```text
WannierBerri/ray_Qiming/
├── main_wb.py                       # WannierBerri 主计算脚本模板
├── ray_memory_monitor.py            # Ray 集群信息、worker 探针和内存监控工具
├── ray_launch_cluster_monitored.sh  # 在 LSF 分配的节点上启动/停止 Ray 集群
└── wbsub_ray_monitored.lsf          # LSF 提交脚本示例
```

## 功能概览

- 在 LSF 作业分配的多节点资源上启动 Ray head 和 worker。
- 运行 WannierBerri 的 `wb.run(..., parallel=True)` 并使用 Ray 并行。
- 将 BLAS/OpenMP 线程数默认限制为 1，减少 Ray worker 内部的嵌套并行和内存压力。
- 可选运行 Ray worker placement probe，检查 Ray 是否真的创建并调度到足够的 worker。
- 周期性记录每个 Ray 节点的内存占用和占用最高的 Ray 相关进程。
- 计算结束或失败后尽量清理 Ray 集群。

## 环境要求

脚本假设集群使用 LSF，并且计算环境中已经安装：

- Python
- Conda 或等价的 Python 环境管理工具
- WannierBerri，示例按 `v1.8` 编写
- Ray
- NumPy / Matplotlib
- psutil，用于内存监控
- LSF 命令：`bsub`、`blaunch`、`bkill`

如果缺少 `psutil`，可在对应环境中安装：

```bash
pip install psutil
```

## 使用前需要修改的内容

这些脚本包含占位路径，提交前必须替换成你自己的集群路径。

### `main_wb.py`

需要修改：

```python
ray_tmp = "/YOUR/PATH/TO/ray_tmp"
wb_tmp = "/YOUR/PATH/TO/wb_Klist_MQM"
res_dir = "/YOUR/PATH/TO/result"

system = wb.System_tb(
    tb_file="/YOUR/PATH/TO/tb_files/wannier90_tb.dat",
    ...
)
```

其中：

- `ray_tmp`：Ray 临时目录，建议放在工作盘或 scratch，避免使用系统 `/tmp`。
- `wb_tmp`：WannierBerri 保存/读取 K 点列表的目录。
- `res_dir`：计算结果、Ray 内存日志输出目录。
- `tb_file`：Wannier90 tight-binding 文件，例如 `wannier90_tb.dat`。

还需要按体系修改：

- `system.set_pointgroup(...)`
- `efermi`
- `FermiDiracSmoother`
- `wb.Grid(system, NK=..., NKFFT=...)`
- `calculators`
- `fout_name`

当前模板计算的是 Gao-Xiao 轨道四极矩相关量：

```python
calculators = {
    "Qorb_total": calc.static.Qorb_GaoXiao(**kwargs),
    "Qorb_CL": calc.static.Qorb_GaoXiao_CL(**kwargs),
    "Qorb_IMD": calc.static.Qorb_GaoXiao_IMD(**kwargs),
    "Qorb_ME": calc.static.Qorb_GaoXiao_ME(**kwargs),
    "Qorb_QMD": calc.static.Qorb_GaoXiao_QMD(**kwargs),
}
```

### `wbsub_ray_monitored.lsf`

需要修改：

```bash
#BSUB -J test
#BSUB -q 2t50c
#BSUB -n 128
#BSUB -R "span[ptile=64]"

source /YOUR/PATH/TO/anaconda3/etc/profile.d/conda.sh
conda activate /YOUR/PATH/TO/anaconda3/envs/wberri_v1.8

export RAY_TMPDIR="/YOUR/PATH/TO/ray_tmp"
```

根据你的集群和任务规模调整：

- `#BSUB -q`：队列名。
- `#BSUB -n`：总 CPU 核数。
- `#BSUB -R "span[ptile=...]"`：每节点核数。
- `OBJECT_STORE_BYTES`：每个 Ray 节点的 object store 内存，单位是 byte。

## 运行方式

进入脚本目录：

```bash
cd WannierBerri/ray_Qiming
```

提交 LSF 作业：

```bash
bsub < wbsub_ray_monitored.lsf
```

`wbsub_ray_monitored.lsf` 会调用：

```bash
sh ./ray_launch_cluster_monitored.sh \
    -c "python -u main_wb.py 2-nodes" \
    -n "wberri_v1.8" \
    -m "$OBJECT_STORE_BYTES" \
    > job.log 2>&1
```

`ray_launch_cluster_monitored.sh` 的参数含义：

| 参数 | 含义 |
| --- | --- |
| `-c` | Ray 集群启动后执行的用户命令 |
| `-n` | Conda 环境名或路径；为空时假设当前环境已有 Ray |
| `-m` | 每个 Ray 节点的 object store memory，单位 byte |

脚本会自动读取 LSF 环境变量中的节点和 CPU 信息：

- `LSB_DJOB_HOSTFILE`
- `LSB_MCPU_HOSTS`
- `LSB_AFFINITY_HOSTFILE`

然后在第一个节点启动 Ray head，在其余节点启动 Ray worker。

## 本地或单节点调试

如果直接运行 `main_wb.py`，脚本设计为启动本地 Ray，并使用下面的环境变量控制本地 CPU 数和 object store 内存：

```bash
export WB_RAY_NUM_CPUS_LOCAL=16
export WB_OBJECT_STORE_GB=20
python -u main_wb.py
```

注意：直接运行仍然需要先把 `main_wb.py` 里的路径和体系参数改好。

## 监控与输出

主要输出包括：

- LSF 标准输出：`%J.out`
- LSF 标准错误：`%J.err`
- Ray 启动和用户命令日志：`job.log`
- WannierBerri 结果：`res_dir` 下以 `CrSe_Qorb_GaoXiao` 为前缀的文件
- Ray 内存监控日志：`res_dir/ray_memory_watch.jsonl`

内存监控每隔 `WB_RAY_MONITOR_INTERVAL` 秒记录一次 JSONL。每条记录包含：

- 每个节点的总内存、已用内存、可用内存和使用百分比。
- Ray 相关进程中 RSS 最高的若干进程。
- `RSS`、`SHR`、`RSS - SHR` 的粗略 heap 估计。
- 若可用，还会记录 `USS` 和 `PSS`。

计算完成后可以汇总内存峰值：

```bash
python ray_memory_monitor.py /YOUR/PATH/TO/result/ray_memory_watch.jsonl --topn 20
```

## 常用环境变量

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `RAY_TMPDIR` | `/tmp/ray-$USER` | Ray 临时目录 |
| `OMP_NUM_THREADS` | `1` | OpenMP 线程数 |
| `MKL_NUM_THREADS` | `1` | MKL 线程数 |
| `OPENBLAS_NUM_THREADS` | `1` | OpenBLAS 线程数 |
| `NUMEXPR_NUM_THREADS` | `1` | NumExpr 线程数 |
| `VECLIB_MAXIMUM_THREADS` | `1` | vecLib 线程数 |
| `RAY_DEDUP_LOGS` | `0` | 是否去重 Ray 日志 |
| `WB_RAY_MONITOR_INTERVAL` | `10` | 内存监控间隔，单位秒 |
| `WB_RAY_MONITOR_TOPN` | `20` | 每个节点写入日志的最大进程数 |
| `WB_RAY_MONITOR_PRINT_TOPN` | `5` | 每次在 stdout 打印的最大进程数 |
| `WB_RAY_WARN_PERCENT` | `90` | 节点内存使用率告警阈值 |
| `WB_RAY_PROBE` | `0` | 是否在计算前运行 Ray worker probe |
| `WB_RAY_PROBE_N` | `WB_RAY_NUM_CPUS_LOCAL` | probe 任务数量 |
| `WB_RAY_NUM_CPUS_LOCAL` | `16` | 直接运行 Python 时本地 Ray CPU 数 |
| `WB_OBJECT_STORE_GB` | `20` | 直接运行 Python 时本地 Ray object store 内存，单位 GB |

## 调试建议

### 检查 Ray worker 是否正常分布

首次在新队列或新节点数上运行时，可以打开：

```bash
export WB_RAY_PROBE=1
export WB_RAY_PROBE_N=64
```

日志中会打印每个 probe 任务所在的 hostname、PID、node ID 和线程环境变量。确认没有问题后，可设为：

```bash
export WB_RAY_PROBE=0
```

### Ray object store 内存不足

如果 Ray 报 object store 相关错误，可调整 `wbsub_ray_monitored.lsf` 中的：

```bash
OBJECT_STORE_BYTES=20000000000
```

该值是每个 Ray 节点的 object store 内存，单位 byte。不要超过单节点可用内存中合理的一部分。

### 节点内存接近耗尽

查看 `job.log` 中的 `[Ray memory monitor]` 输出，或汇总 `ray_memory_watch.jsonl`。如果某些 worker RSS 持续升高，可以尝试：

- 减小 `NK` 或 `NKFFT`。
- 调小 WannierBerri 的自适应迭代参数。
- 增加节点数或单节点内存。
- 调整 `OBJECT_STORE_BYTES`，避免 object store 和 worker heap 互相挤占。

### 嵌套线程导致过度占用

模板已经把常见 BLAS/OpenMP 线程数设为 1。若你在其他入口脚本中导入 NumPy/SciPy/WannierBerri，请确保这些环境变量在导入前就已经设置。

## 当前代码注意事项

`main_wb.py` 使用 `connect_or_init_ray_for_wberri(...)` 来连接外部 Ray 集群或初始化本地 Ray。请确保本地的 `ray_memory_monitor.py` 中包含该函数；如果你的版本只有 `init_ray_for_wberri(...)`，需要同步 helper 文件或把 `main_wb.py` 改成对应的初始化方式后再运行。
