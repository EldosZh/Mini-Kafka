from typer.testing import CliRunner

from event_streaming.cli import app


def test_help_and_version():
    runner = CliRunner()
    assert runner.invoke(app, ["--help"]).exit_code == 0
    version = runner.invoke(app, ["--version"])
    assert version.exit_code == 0
    assert "0.1.0" in version.stdout
    for command in ("broker", "topic", "group", "storage", "benchmark"):
        assert runner.invoke(app, [command, "--help"]).exit_code == 0
