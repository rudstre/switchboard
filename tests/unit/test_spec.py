import json
import time

import pytest

from switchboard.spec import SpecError, expand, parse_spec, render_command

BASE = """
experiment: demo
command: ["run", "--a", "{param.alpha}", "--seed", "{seed}", "--out", "{outdir}"]
backends: {local: {workdir: ., output_root: /tmp/x}}
"""


def load(text, tmp_path):
    return parse_spec(text, base_dir=tmp_path)


def test_grid_times_seeds_in_stable_order(tmp_path):
    s = load(BASE + "params: {grid: {alpha: [1, 2], beta: [x, y, z]}, seeds: [7, 8]}", tmp_path)
    tasks = expand(s)
    assert len(tasks) == 2 * 3 * 2
    assert tasks[0].params == {"alpha": 1, "beta": "x"} and tasks[0].seed == 7
    assert tasks[1].seed == 8
    assert [t.idx for t in tasks] == list(range(12))
    assert [t.key for t in expand(load(BASE + "params: {grid: {alpha: [1, 2], beta: [x, y, z]}, seeds: [7, 8]}",
                                       tmp_path))] == [t.key for t in tasks]


def test_linspace_range_and_zip(tmp_path):
    s = load(BASE + "params: {grid: {alpha: {linspace: [0, 1, 5]}, n: {range: [0, 6, 2]}}, "
                    "zip: {a: [1, 2], b: [3, 4]}, seeds: [0]}", tmp_path)
    tasks = expand(s)
    alphas = sorted({t.params["alpha"] for t in tasks})
    assert alphas[0] == 0.0 and alphas[-1] == 1.0 and len(alphas) == 5
    assert sorted({t.params["n"] for t in tasks}) == [0, 2, 4]
    assert len(tasks) == 5 * 3 * 2
    assert {(t.params["a"], t.params["b"]) for t in tasks} == {(1, 3), (2, 4)}


def test_manifest_rows_with_their_own_seeds(tmp_path):
    (tmp_path / "tasks.jsonl").write_text('{"alpha": 1, "seed": 5}\n\n{"alpha": 2, "seed": 6}\n')
    s = load(BASE + "params: {manifest: tasks.jsonl}", tmp_path)
    tasks = expand(s)
    assert [(t.params, t.seed) for t in tasks] == [({"alpha": 1}, 5), ({"alpha": 2}, 6)]
    assert s.manifest_sha256


def test_ten_thousand_tasks_expand_quickly(tmp_path):
    s = load(BASE + "params: {grid: {alpha: {range: [0, 100]}, beta: {range: [0, 100]}}, seeds: [1]}", tmp_path)
    t0 = time.perf_counter()
    tasks = expand(s)
    assert len(tasks) == 10_000
    assert time.perf_counter() - t0 < 1.0
    assert len({t.key for t in tasks}) == 10_000


def test_spec_hash_ignores_non_identity_fields(tmp_path):
    a = load(BASE + "params: {grid: {alpha: [1]}, seeds: [1]}\nresources: {cpus: 1}", tmp_path)
    b = load(BASE + "params: {grid: {alpha: [1]}, seeds: [1]}\nresources: {cpus: 8, mem_gb: 30}\n"
                    "retries: {max: 3}\ninterruptible: true", tmp_path)
    c = load(BASE.replace('"run"', '"run2"') + "params: {grid: {alpha: [1]}, seeds: [1]}", tmp_path)
    assert a.spec_hash() == b.spec_hash()
    assert a.spec_hash() != c.spec_hash()


@pytest.mark.parametrize("bad, message", [
    ('command: ["x", "{nope}"]', "unknown placeholder"),
    ('command: ["x", "{param.gamma}"]', "does not name a parameter"),
    ('command: ["x", "{seed}"]\nparams: {grid: {alpha: [1]}}', "no seed"),
])
def test_template_errors_are_reported_at_load(tmp_path, bad, message):
    text = "experiment: demo\nbackends: {local: {workdir: ., output_root: /tmp}}\n" + bad
    if "params" not in bad:
        text += "\nparams: {grid: {alpha: [1]}, seeds: [1]}"
    with pytest.raises(SpecError, match=message):
        load(text, tmp_path)


def test_schema_errors_are_readable(tmp_path):
    with pytest.raises(SpecError, match="resources.cpus"):
        load(BASE + "params: {seeds: [1], grid: {alpha: [1]}}\nresources: {cpus: 0}", tmp_path)
    with pytest.raises(SpecError, match="exactly one provider"):
        load(BASE + "params: {seeds: [1], grid: {alpha: [1]}}\nidentity: {code: {}}", tmp_path)


def test_render_command():
    argv = render_command(["a", "{param.alpha:.2f}", "{seed}", "{{literal}}"], {"param": {"alpha": 0.123}, "seed": 4})
    assert argv == ["a", "0.12", "4", "{literal}"]
    assert json.loads(json.dumps(argv)) == argv


def test_yaml_on_key_is_a_string_not_a_boolean(tmp_path):
    s = load(BASE + "params: {grid: {alpha: [1]}, seeds: [1]}\nretries: {max: 1, on: [nonzero]}\n"
                    "interruptible: true",
             tmp_path)
    assert s.spec.retries.on == ["nonzero"] and s.spec.interruptible is True
