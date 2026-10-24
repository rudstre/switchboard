import pytest

from switchboard.runner.fingerprint import FingerprintError, command_fingerprint, git_fingerprint

from ..conftest import git


def fp(repo, paths=(".",)):
    return git_fingerprint(str(repo), list(paths))


def test_clean_tree_is_deterministic(repo):
    assert fp(repo) == fp(repo)
    assert fp(repo).startswith("git:")


def test_every_edit_changes_the_id_including_repeated_edits(repo):
    clean = fp(repo)
    (repo / "src" / "model.py").write_text("X = 2\n")
    first = fp(repo)
    # The v2 plan used `git status --porcelain`, which is identical for both of these states.
    (repo / "src" / "model.py").write_text("X = 3\n")
    second = fp(repo)
    assert len({clean, first, second}) == 3


def test_staged_changes_count(repo):
    (repo / "src" / "model.py").write_text("X = 2\n")
    unstaged = fp(repo)
    git(repo, "add", "-A")
    assert fp(repo) == unstaged  # same content, staged or not


def test_untracked_contents_count_but_ignored_files_do_not(repo):
    clean = fp(repo)
    (repo / "debug.log").write_text("noise")
    assert fp(repo) == clean
    (repo / "src" / "new.py").write_text("a")
    with_new = fp(repo)
    (repo / "src" / "new.py").write_text("b")
    assert len({clean, with_new, fp(repo)}) == 3


def test_paths_scope_the_fingerprint(repo):
    scoped = fp(repo, ["src"])
    (repo / "README.md").write_text("docs")
    assert fp(repo, ["src"]) == scoped
    (repo / "src" / "model.py").write_text("X = 9\n")
    assert fp(repo, ["src"]) != scoped


def test_new_commit_changes_the_id(repo):
    clean = fp(repo)
    (repo / "src" / "model.py").write_text("X = 2\n")
    git(repo, "commit", "-qam", "edit")
    assert fp(repo) != clean


def test_command_fingerprint_uses_setup_and_reports_errors(tmp_path):
    a = command_fingerprint(str(tmp_path), "echo $FOO", setup="export FOO=1")
    b = command_fingerprint(str(tmp_path), "echo $FOO", setup="export FOO=2")
    assert a != b and a.startswith("cmd:")
    with pytest.raises(FingerprintError, match="exited 3"):
        command_fingerprint(str(tmp_path), "exit 3")


def test_not_a_repository(tmp_path):
    with pytest.raises(FingerprintError):
        git_fingerprint(str(tmp_path), ["."])
