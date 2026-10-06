defmodule IrohBeam.TestRelay do
  @moduledoc """
  Controls the local test relay: `docker compose` by default, or the native
  `iroh-relay` binary named by `IROH_BEAM_RELAY_BIN`. Each call returns
  `{output, exit_status}` like `System.cmd/3`.
  """

  @token "iroh-beam-local-test-token-not-for-production"

  def up do
    case System.get_env("IROH_BEAM_RELAY_BIN") do
      nil -> compose(["up", "--detach", "iroh-relay"])
      bin -> if running?(), do: {"", 0}, else: spawn_bin(bin)
    end
  end

  def stop do
    case System.get_env("IROH_BEAM_RELAY_BIN") do
      nil ->
        compose(["stop", "iroh-relay"])

      _bin ->
        sh(
          ~s|pid=$(cat "$1") && kill "$pid" && while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done; rm -f "$1"|
        )
    end
  end

  def restart do
    {output, 0} = stop()
    {more, status} = up()
    {output <> more, status}
  end

  @doc "Where the native relay's OS pid is kept (also read by bin/qa_check.sh)."
  def pid_file, do: Path.join(System.tmp_dir!(), "iroh-beam-test-relay.pid")

  defp compose(args), do: System.cmd("docker", ["compose" | args], stderr_to_stdout: true)

  defp spawn_bin(bin) do
    output = Path.join(System.tmp_dir!(), "iroh-beam-test-relay.out")

    System.cmd(
      "sh",
      [
        "-c",
        ~s|nohup "$2" --dev --config-path test/fixtures/iroh-relay-local.toml > "$3" 2>&1 & echo $! > "$1"|,
        "sh",
        pid_file(),
        bin,
        output
      ],
      env: [{"IROH_RELAY_ACCESS_TOKEN", @token}],
      stderr_to_stdout: true
    )
  end

  defp running?, do: match?({_, 0}, sh(~s|[ -s "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null|))

  defp sh(script), do: System.cmd("sh", ["-c", script, "sh", pid_file()], stderr_to_stdout: true)
end
