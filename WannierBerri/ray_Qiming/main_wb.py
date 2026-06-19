import os

# Set these before importing numpy/scipy-heavy modules.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")

import wannierberri as wb
from wannierberri import calculators as calc
import numpy as np
import matplotlib.pyplot as plt
import ray
from wannierberri.smoother import FermiDiracSmoother
from wannierberri import symmetry as symm

from ray_memory_monitor import (
    connect_or_init_ray_for_wberri,
    start_ray_memory_watch,
    print_ray_cluster_info,
    run_ray_worker_probe,
    ray_shutdown_safe,
)

print(f"Using WannierBerri version {wb.__version__}")

################################################################################
#                            Paths
################################################################################

ray_tmp = "/YOUR/PATH/TO/ray_tmp"
wb_tmp = "/YOUR/PATH/TO/wb_Klist_MQM"
res_dir = "/YOUR/PATH/TO/result"

os.makedirs(ray_tmp, exist_ok=True)
os.makedirs(wb_tmp, exist_ok=True)
os.makedirs(res_dir, exist_ok=True)

################################################################################
#                            Setting ray + worker monitor
################################################################################

# If this script is launched through ray_launch_cluster.sh, the script exports
# ip_head/RAY_ADDRESS, and connect_or_init_ray_for_wberri() will connect to that
# existing multi-node Ray cluster.
#
# If you run this Python script directly without ray_launch_cluster.sh, it will
# start a local Ray instance using num_cpus below.
RAY_NUM_CPUS_LOCAL = int(os.environ.get("WB_RAY_NUM_CPUS_LOCAL", "16"))

ray_mode = connect_or_init_ray_for_wberri(
    num_cpus=RAY_NUM_CPUS_LOCAL,
    temp_dir=ray_tmp,
    include_dashboard=False,
    # For local mode only.  For externally launched clusters, object-store memory
    # is controlled by ray_launch_cluster.sh via ray start --object-store-memory.
    object_store_memory_gb=float(os.environ.get("WB_OBJECT_STORE_GB", "20")),
)

print(f"[CrSe_MQM] Ray mode = {ray_mode}")
print_ray_cluster_info()

# Optional: probe real worker placement before WannierBerri starts.
# Use this if you want to check whether Ray can really create enough workers.
if os.environ.get("WB_RAY_PROBE", "0") == "1":
    nprobe = int(os.environ.get("WB_RAY_PROBE_N", str(RAY_NUM_CPUS_LOCAL)))
    run_ray_worker_probe(num_probe=nprobe, sleep_time=5.0)

mem_watch = start_ray_memory_watch(
    log_file=os.path.join(res_dir, "ray_memory_watch.jsonl"),
    interval_sec=float(os.environ.get("WB_RAY_MONITOR_INTERVAL", "10")),
    topn=int(os.environ.get("WB_RAY_MONITOR_TOPN", "20")),
    warn_node_used_percent=float(os.environ.get("WB_RAY_WARN_PERCENT", "90")),
    print_topn=int(os.environ.get("WB_RAY_MONITOR_PRINT_TOPN", "5")),
    append=True,
)

################################################################################
#                            Loading tb.dat
################################################################################

system = wb.System_tb(
    tb_file="/YOUR/PATH/TO/tb_files/wannier90_tb.dat",
    qorb=True,
)

system.set_pointgroup(["Inversion*TimeReversal", "C6z", "C2x"])
nsym = getattr(system.pointgroup, "nsym", None) or getattr(system.pointgroup, "size", None) or len(system.pointgroup)
print("Number of symmetry operations: ", nsym)
print("nRvec: ", system.rvec.nRvec)

################################################################################
#                            Setting calculators
################################################################################

efermi = np.linspace(6.7412, 7.7412, 201, True)
smoother = FermiDiracSmoother(efermi, 0.1)

kwargs = dict(
    Efermi=efermi,
    smoother=smoother,
)

grid = wb.Grid(system, NK=300, NKFFT=6)

calculators = {
    "Qorb_total": calc.static.Qorb_GaoXiao(**kwargs),
    "Qorb_CL": calc.static.Qorb_GaoXiao_CL(**kwargs),
    "Qorb_IMD": calc.static.Qorb_GaoXiao_IMD(**kwargs),
    "Qorb_ME": calc.static.Qorb_GaoXiao_ME(**kwargs),
    "Qorb_QMD": calc.static.Qorb_GaoXiao_QMD(**kwargs),
}

try:
    result = wb.run(
        system,
        grid=grid,
        calculators=calculators,
        print_Kpoints=False,
        parallel=True,    # using ray
        adpt_num_iter=20,
        fout_name=os.path.join(res_dir, "CrSe_Qorb_GaoXiao"),
        file_Klist_path=wb_tmp,
        restart=False,
        dump_results=True,
        allow_restart=True,
        use_irred_kpt=True,
        symmetrize=True,
        print_progress_step_percent=10,
    )
finally:
    # Always stop the monitor cleanly even if WannierBerri fails/OOMs.
    mem_watch.stop()
    ray_shutdown_safe()

# Keep the same post-run access pattern as your original script.
result.results["Qorb_total"].dataSmooth
result.results["Qorb_CL"].dataSmooth
result.results["Qorb_IMD"].dataSmooth
result.results["Qorb_ME"].dataSmooth
result.results["Qorb_QMD"].dataSmooth
