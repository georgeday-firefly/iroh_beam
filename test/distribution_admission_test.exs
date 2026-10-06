defmodule IrohBeam.DistributionAdmissionTest do
  use IrohBeam.FixtureCase, async: false

  alias IrohBeam.{DistributionProcess, Identity, Relay, SecretKey, TestRelay}

  @moduletag :relay
  @relay_url "http://127.0.0.1:3340"
  @token "iroh-beam-local-test-token-not-for-production"
  @cookie "admission_cookie"

  setup do
    {_output, 0} = TestRelay.up()
    assert eventually(&relay_ready?/0, 150)
    {:ok, relay} = Relay.new(@relay_url, token: @token)
    %{relay: relay}
  end

  test "name@<endpoint id> peers connect with no peer map; admission is checked and live",
       %{tmp_dir: tmp_dir, relay: relay} do
    a = peer(tmp_dir, "a", relay)
    b = peer(tmp_dir, "b", relay)
    c = peer(tmp_dir, "c", relay)

    {:ok, pa} = start(a, b, admit: [b])
    {:ok, pb} = start(b, a, admit: [a])

    assert {:ok, pb} = DistributionProcess.send(pb, "CONNECT\nPING\nRPC\n")
    assert {:ok, pb} = DistributionProcess.await_output(pb, "CONNECT true", 20_000)
    assert {:ok, pb} = DistributionProcess.await_output(pb, "PING :pong", 5_000)
    assert {:ok, pb} = DistributionProcess.await_output(pb, "RPC #{inspect(a.node)}", 5_000)

    assert {:ok, pb} = DistributionProcess.send(pb, "PATHEVENT\n")
    assert {:ok, pb} = DistributionProcess.await_output(pb, "PATHEVENT {:", 10_000)
    assert DistributionProcess.output(pb) =~ ~r/PATHEVENT \{:(direct|relay), true\}/

    assert {:ok, pb} = DistributionProcess.send(pb, "LARGE50\n")
    assert {:ok, pb} = DistributionProcess.await_output(pb, "LARGE50 true nodedowns=0", 90_000)

    # c is not admitted by a yet: refused at the endpoint ID, before any preface.
    {:ok, pc} = start(c, a, admit: [a])
    assert {:ok, pc} = DistributionProcess.send(pc, "CONNECT_ONCE\n")
    assert {:ok, pc} = DistributionProcess.await_output(pc, "CONNECT false", 20_000)
    assert {:ok, pa} = DistributionProcess.send(pa, "REJECTIONS\n")
    assert {:ok, pa} = DistributionProcess.await_output(pa, "REJECTIONS [:endpoint_id", 5_000)

    assert {:ok, pa} = DistributionProcess.send(pa, "ADMIT #{c.id} c\n")
    assert {:ok, pa} = DistributionProcess.await_output(pa, "ADMIT ok", 5_000)
    pc = %{pc | output: ""}
    assert {:ok, pc} = DistributionProcess.send(pc, "CONNECT\nPING\n")
    assert {:ok, pc} = DistributionProcess.await_output(pc, "CONNECT true", 20_000)
    assert {:ok, pc} = DistributionProcess.await_output(pc, "PING :pong", 5_000)

    Enum.each([pa, pb, pc], &stop/1)
  end

  test "relay-only name@<endpoint id> link reports a relay path", %{
    tmp_dir: tmp_dir,
    relay: relay
  } do
    relay_only = [direct_ip: false, bind: []]
    a = peer(tmp_dir, "a", relay, relay_only)
    b = peer(tmp_dir, "b", relay, relay_only)

    {:ok, pa} = start(a, b, admit: [b])
    {:ok, pb} = start(b, a, admit: [a])

    assert {:ok, pb} = DistributionProcess.send(pb, "CONNECT\nINFO\nPING\n")
    assert {:ok, pb} = DistributionProcess.await_output(pb, "CONNECT true", 20_000)
    assert {:ok, pb} = DistributionProcess.await_output(pb, "kind: :relay", 5_000)
    assert {:ok, pb} = DistributionProcess.await_output(pb, "PING :pong", 5_000)

    Enum.each([pa, pb], &stop/1)
  end

  test "an admitted key claiming another key's name is refused without creating the atom",
       %{tmp_dir: tmp_dir, relay: relay} do
    a = peer(tmp_dir, "a", relay)
    b = peer(tmp_dir, "b", relay)

    # mallory holds its own key but uses b's endpoint ID in its node name. It runs
    # with a static peer map (no admission), which skips the own-name check.
    mallory_key = Path.join(tmp_dir, "mallory.key")
    mallory_id = endpoint_id(mallory_key)
    mallory_node = String.to_atom("mallory@#{b.id}")

    mallory_options =
      options(mallory_node, mallory_key, relay,
        admission: nil,
        peers: %{a.node => {:addr, %{endpoint_id: a.id, relay_urls: [@relay_url]}}}
      )

    mallory = %{
      node: mallory_node,
      id: mallory_id,
      path: write_options(tmp_dir, "mallory.options", mallory_options)
    }

    {:ok, pa} = start(a, b, admit: [%{mallory | node: :mallory@x}])
    {:ok, pm} = start(mallory, a, admit: [])

    assert {:ok, pm} = DistributionProcess.send(pm, "CONNECT_ONCE\n")
    assert {:ok, pm} = DistributionProcess.await_output(pm, "CONNECT false", 20_000)
    assert {:ok, pa} = DistributionProcess.send(pa, "REJECTIONS\nATOM mallory@#{b.id}\n")
    assert {:ok, pa} = DistributionProcess.await_output(pa, "REJECTIONS [:name_binding]", 5_000)
    assert {:ok, pa} = DistributionProcess.await_output(pa, "ATOM false", 5_000)

    Enum.each([pa, pm], &stop/1)
  end

  test "a local inet_tcp client can rpc a name@<endpoint id> node beside the iroh carrier",
       %{tmp_dir: tmp_dir, relay: relay} do
    a = peer(tmp_dir, "a", relay)
    b = peer(tmp_dir, "b", relay)
    port = Integer.to_string(free_tcp_port())

    {:ok, pa} =
      start(a, b,
        admit: [],
        erl:
          "-proto_dist iroh inet_tcp -no_epmd -epmd_module iroh_loopback_epmd " <>
            "-kernel inet_dist_use_interface {127,0,0,1}",
        env: [{"IROH_BEAM_LOOPBACK_PORT", port}]
      )

    ebin = Path.join(Mix.Project.build_path(), "lib/iroh_beam/ebin")

    {output, status} =
      System.cmd(
        "elixir",
        [
          "--erl",
          "-proto_dist inet_tcp -epmd_module iroh_loopback_epmd -start_epmd false " <>
            "-dist_listen false -pa #{ebin}",
          "--sname",
          "rpc#{System.unique_integer([:positive])}",
          "--hidden",
          "--cookie",
          @cookie,
          "-e",
          "IO.inspect(:erpc.call(:\"#{a.node}\", :erlang, :node, []))"
        ],
        env: [{"IROH_BEAM_LOOPBACK_PORT", port}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ inspect(a.node)

    stop(pa)
  end

  defp peer(tmp_dir, label, relay, extra \\ []) do
    key = Path.join(tmp_dir, "#{label}.key")
    id = endpoint_id(key)
    node = String.to_atom("#{label}@#{id}")
    path = write_options(tmp_dir, "#{label}.options", options(node, key, relay, extra))
    %{node: node, id: id, path: path}
  end

  defp options(node, key, relay, extra) do
    {:ok, port} = free_udp_port()

    Keyword.merge(
      [
        name: node,
        name_domain: :shortnames,
        identity: {:file, key},
        network: {:custom, [relay]},
        bind: ["127.0.0.1:#{port}"],
        peers: %{},
        admission: {IrohBeam.TestAdmission, :allowed?},
        startup_timeout: 10_000,
        shutdown_timeout: 2_000,
        connect_timeout: 10_000,
        accept_timeout: 100,
        stream_timeout: 10_000,
        net_ticktime: 4,
        net_tickintensity: 4
      ],
      extra
    )
  end

  # Starts a separate BEAM for `local` that dials `peer` and admits each `admit`
  # entry's endpoint ID under the name part of its node.
  defp start(local, peer, opts) do
    admitted =
      Enum.map_join(Keyword.fetch!(opts, :admit), ";", fn %{node: node, id: id} ->
        [name, _host] = String.split(Atom.to_string(node), "@")
        "#{id}=#{name}"
      end)

    {:ok, process} =
      DistributionProcess.start(
        "elixir",
        [
          "--erl",
          Keyword.get(opts, :erl, "-proto_dist iroh -no_epmd"),
          "-S",
          "mix",
          "run",
          "--no-compile",
          "test/support/distribution_peer.exs"
        ],
        env:
          [
            {"MIX_ENV", "test"},
            {"IROH_BEAM_DISTRIBUTION_OPTIONS", local.path},
            {"IROH_BEAM_DISTRIBUTION_COOKIE", @cookie},
            {"IROH_BEAM_DISTRIBUTION_PEER", Atom.to_string(peer.node)},
            {"IROH_BEAM_TEST_ADMIT", admitted}
          ] ++ Keyword.get(opts, :env, [])
      )

    DistributionProcess.await_output(process, "PEER_READY #{local.node}", 20_000)
  end

  defp stop(process) do
    {:ok, process} = DistributionProcess.send(process, "STOP\n")
    DistributionProcess.await_exit(process, 10_000)
  end

  defp endpoint_id(path) do
    {:ok, key} = Identity.load_or_create(path)
    {:ok, id} = SecretKey.endpoint_id(key)
    to_string(id)
  end

  defp free_udp_port do
    {:ok, socket} = :gen_udp.open(0, ip: {127, 0, 0, 1})
    {:ok, {_ip, port}} = :inet.sockname(socket)
    :gen_udp.close(socket)
    {:ok, port}
  end

  defp free_tcp_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp write_options(tmp_dir, name, options) do
    path = Path.join(tmp_dir, name)
    File.write!(path, :erlang.term_to_binary(options))
    path
  end

  defp eventually(fun, attempts) do
    cond do
      fun.() ->
        true

      attempts > 1 ->
        Process.sleep(100)
        eventually(fun, attempts - 1)

      true ->
        false
    end
  end

  defp relay_ready? do
    case :gen_tcp.connect(~c"127.0.0.1", 3340, [:binary, active: false], 1_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _reason} ->
        false
    end
  end
end
