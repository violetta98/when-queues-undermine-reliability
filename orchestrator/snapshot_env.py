#!/usr/bin/env python3
"""Record the testbed: host hardware, OS, Docker, VM resources, broker versions.

Writes artifact/env/host.json plus the raw command output it was derived from,
so the methodology section can be checked against something rather than trusted.
"""

import json
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / "env"
ENV.mkdir(exist_ok=True)


def run(cmd):
    p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True)
    return p.stdout.strip()


MACOS = platform.system() == "Darwin"

# The same facts are collected on either host OS, with the commands that exist
# there. Keeping both branches in one file means a Linux rerun produces a
# host.json with exactly the same keys, so the two testbeds can be compared
# field by field rather than by reading prose.
COMMON = {
    "uname": "uname -a",
    "df_root": "df -k /",
    "docker_version": "docker version --format '{{.Server.Version}} api={{.Server.APIVersion}}'",
    "docker_info": "docker info --format '{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.Driver}}|{{.CgroupVersion}}|{{.KernelVersion}}|{{.OperatingSystem}}'",
    "go_version": "go version",
    "python_version": "python3 --version",
}
MAC_ONLY = {
    "sw_vers": "sw_vers",
    "cpu_brand": "sysctl -n machdep.cpu.brand_string",
    "ncpu": "sysctl -n hw.ncpu",
    "physicalcpu": "sysctl -n hw.physicalcpu",
    "memsize": "sysctl -n hw.memsize",
    "docker_desktop": "defaults read com.docker.docker 2>/dev/null | head -40",
    "disk_model": "system_profiler SPNVMeDataType 2>/dev/null | head -25",
}
LINUX_ONLY = {
    "os_release": "cat /etc/os-release",
    "cpu_brand": "awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo",
    "ncpu": "nproc",
    "physicalcpu": "lscpu -p=Core,Socket 2>/dev/null | grep -v '^#' | sort -u | wc -l",
    "memsize": "awk '/MemTotal/{print $2*1024}' /proc/meminfo",
    "disk_model": "lsblk -do NAME,MODEL,ROTA,SIZE 2>/dev/null | head -10",
    # On a cloud VM these three decide whether fsync is cheap or dear, which is
    # what the laptop testbed could not pin down.
    "disk_scheduler": "for q in /sys/block/*/queue/scheduler; do echo \"$q $(cat $q)\"; done 2>/dev/null | head -5",
    "mount_data": "findmnt -no SOURCE,FSTYPE,OPTIONS --target /var/lib/docker",
    "virt": "systemd-detect-virt 2>/dev/null || echo unknown",
}
RAW = {**COMMON, **(MAC_ONLY if MACOS else LINUX_ONLY)}

raw = {k: run(v) for k, v in RAW.items()}
(ENV / "raw_commands.json").write_text(json.dumps(
    {k: {"cmd": RAW[k], "stdout": v} for k, v in raw.items()}, indent=2))

info = raw["docker_info"].split("|") + [""] * 7
host = {
    "machine": "Apple MacBook Pro (Intel)" if MACOS else raw.get("virt", "").strip() or "Linux host",
    "cpu": raw["cpu_brand"],
    "cpu_logical": int(raw["ncpu"] or 0),
    "cpu_physical": int(raw["physicalcpu"] or 0),
    "ram_bytes": int(raw["memsize"] or 0),
    "os": " ".join(raw["sw_vers"].split()) if MACOS
          else " ".join(l.split("=", 1)[1].strip('"') for l in raw["os_release"].splitlines()
                        if l.startswith("PRETTY_NAME=")),
    "kernel": raw["uname"],
    "python": raw["python_version"],
    "go": raw["go_version"],
    "docker": {
        "server_version": info[0],
        "vm_cpus": int(info[1] or 0),
        "vm_memory_bytes": int(info[2] or 0),
        "storage_driver": info[3],
        "cgroup_version": info[4],
        "vm_kernel": info[5],
        "vm_os": info[6],
    },
}

# Broker versions, taken from the images actually used.
host["images"] = {}
for image, cmd in (
    ("rabbitmq:4.3.6-management",
     ["docker", "run", "--rm", "--entrypoint", "rabbitmqctl", "rabbitmq:4.3.6-management", "version"]),
    ("apache/kafka:4.3.1",
     ["docker", "run", "--rm", "apache/kafka:4.3.1", "/opt/kafka/bin/kafka-topics.sh", "--version"]),
    ("alpine:3.20", ["docker", "run", "--rm", "alpine:3.20", "cat", "/etc/alpine-release"]),
):
    digest = run(["docker", "image", "inspect", image, "--format", "{{index .RepoDigests 0}}"])
    version = run(cmd)
    host["images"][image] = {
        "digest": digest,
        "version": version.splitlines()[0] if version else "",
        "created": run(["docker", "image", "inspect", image, "--format", "{{.Created}}"]),
    }

(ENV / "host.json").write_text(json.dumps(host, indent=2))
json.dump(host, sys.stdout, indent=2)
print()
