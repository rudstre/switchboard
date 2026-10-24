"""Golden routing scenarios: fixed inputs -> expected splits and reason strings."""

from switchboard.routing import RouteContext, RouteExperiment, RouteTask, local_drain_s, route

HOUR = 3600


def exp(policy="local-first", wait_h=6.0, platforms=("osx-arm64", "linux-64"), has_local=True, has_hpc=True,
        identity=None):
    return {1: RouteExperiment(1, policy, wait_h * HOUR, tuple(platforms), has_local, has_hpc, identity)}


def ctx(cpus=4, profile="focus", reachable=True, configured=True, committed=0.0):
    return RouteContext(capacity_cpus=cpus, profile=profile, max_local_cpus=12, max_local_mem_mb=28 * 1024,
                        hpc_configured=configured, hpc_reachable=reachable, committed_local_cpu_s=committed)


def tasks(n, cpus=1, runtime_min=30, mem_gb=1):
    return [RouteTask(i, 1, cpus, mem_gb * 1024, runtime_min * 60 if runtime_min else None) for i in range(n)]


def split(decisions):
    out = {}
    for d in decisions.values():
        out[d.backend] = out.get(d.backend, 0) + 1
    return out


def test_local_first_overflows_the_tail():
    # 100 x 1 cpu x 30 min; focus = 4 cpus; 6 h limit -> 24 cpu-h fit -> 48 tasks stay local
    ts = tasks(100)
    d = route(ts, exp(), ctx(cpus=4))
    assert split(d) == {"local": 48, "hpc": 52}
    assert [d[i].backend for i in range(100)] == ["local"] * 48 + ["hpc"] * 52  # head local, tail overflow
    assert d[0].note == "[R5] local-first: the Mac finishes it in ~8 min at the 'focus' budget (limit 6.0 h)"
    assert d[99].note == ("[R5] local-first overflow: the Mac would finish it in ~12.5 h at the 'focus' budget, "
                          "over the 6.0 h limit")
    assert local_drain_s(d, ts, ctx(cpus=4)) == 6 * HOUR


def test_more_local_capacity_keeps_more_on_the_mac():
    assert split(route(tasks(100), exp(), ctx(cpus=12, profile="away"))) == {"local": 100}
    assert split(route(tasks(100), exp(wait_h=1), ctx(cpus=12, profile="away"))) == {"local": 24, "hpc": 76}


def test_committed_local_work_counts_first():
    # 2 h of the 6 h already taken by running/manual work -> 4 h x 4 cpus = 32 tasks
    assert split(route(tasks(100), exp(), ctx(committed=2 * HOUR * 4))) == {"local": 32, "hpc": 68}


def test_no_runtime_estimate_stays_local():
    d = route(tasks(10, runtime_min=None), exp(), ctx())
    assert split(d) == {"local": 10}
    assert d[0].rule == "R4" and "resources.runtime_min" in d[0].reason


def test_policies():
    assert split(route(tasks(100), exp("local-only"), ctx())) == {"local": 100}
    assert split(route(tasks(5), exp("hpc-only"), ctx())) == {"hpc": 5}
    waiting = route(tasks(5), exp("hpc-only"), ctx(reachable=False))
    assert split(waiting) == {None: 5} and "the cluster is unreachable" in waiting[0].reason


def test_not_runnable_on_the_mac():
    d = route(tasks(3), exp(platforms=("linux-64",)), ctx())
    assert split(d) == {"hpc": 3} and d[0].note.startswith("[R2] not runnable on osx-arm64")
    big = route(tasks(2, cpus=16), exp(), ctx())
    assert split(big) == {"hpc": 2} and "more than the Mac's largest budget (12 cpus / 28 GB)" in big[0].reason
    stuck = route(tasks(1, cpus=16), exp(), ctx(reachable=False))
    assert stuck[0].backend is None and "waiting" in stuck[0].reason


def test_cluster_unusable_keeps_everything_local():
    for c, e, why in [(ctx(reachable=False), exp(), "the cluster is unreachable"),
                      (ctx(configured=False), exp(), "no cluster configured"),
                      (ctx(), exp(has_hpc=False), "the spec has no backends.hpc"),
                      (ctx(), exp(identity="identity mismatch on hpc: code id changed"), "identity mismatch")]:
        d = route(tasks(100), e, c)
        assert split(d) == {"local": 100}
        assert d[0].rule == "R3" and why in d[0].reason


def test_late_binding_reconsiders_pending_tasks():
    ts = tasks(100)
    away = route(ts, exp(), ctx(cpus=12, profile="away"))
    focus = route(ts, exp(), ctx(cpus=4))
    assert split(away) == {"local": 100} and split(focus) == {"local": 48, "hpc": 52}
