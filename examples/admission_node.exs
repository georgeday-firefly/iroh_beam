# Interactive demo of `name@<endpoint id>` nodes with runtime admission.
#
#   iex --erl "-proto_dist iroh -no_epmd" -S mix run examples/admission_node.exs a
#   iex --erl "-proto_dist iroh -no_epmd" -S mix run examples/admission_node.exs b
#
# Each prints its node name. Then, in each shell, admit the other and connect:
#
#   Demo.admit("<other endpoint id>", "b")
#   Node.connect(:"b@<other endpoint id>")
#   IrohBeam.Distribution.peer_info(:"b@<other endpoint id>")
#
# Uses n0's public relays and address lookup, so it also works across machines.
# To use an iroh-services project instead (your relays, dashboard reporting), set
# IROH_SERVICES_API_SECRET_FILE to a file holding the project API secret, and
# optionally IROH_SERVICES_RELAYS to comma-separated relay URLs.
defmodule Demo do
  @key {__MODULE__, :admitted}

  def admit(id, name), do: :persistent_term.put(@key, Map.put(admitted(), id, name))

  def allowed?(id, nil), do: Map.has_key?(admitted(), id)
  def allowed?(id, name), do: Map.get(admitted(), id) == name

  defp admitted, do: :persistent_term.get(@key, %{})
end

[name] = System.argv()
identity = "tmp/#{name}.iroh"

network =
  case System.get_env("IROH_SERVICES_API_SECRET_FILE") do
    nil ->
      :n0

    secret_file ->
      {:iroh_services,
       api_secret_file: secret_file,
       relays: String.split(System.get_env("IROH_SERVICES_RELAYS", ""), ",", trim: true),
       name: "iroh-beam-demo-#{name}",
       diagnostics: true}
  end

{:ok, key} = IrohBeam.Identity.load_or_create(identity)
{:ok, id} = IrohBeam.SecretKey.endpoint_id(key)

{:ok, _pid} =
  IrohBeam.Distribution.start(
    name: :"#{name}@#{id}",
    identity: {:file, identity},
    network: network,
    admission: {Demo, :allowed?}
  )

Node.set_cookie(:iroh_beam_demo)
IO.puts("node #{Node.self()}")
