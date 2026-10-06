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
defmodule Demo do
  @key {__MODULE__, :admitted}

  def admit(id, name), do: :persistent_term.put(@key, Map.put(admitted(), id, name))

  def allowed?(id, nil), do: Map.has_key?(admitted(), id)
  def allowed?(id, name), do: Map.get(admitted(), id) == name

  defp admitted, do: :persistent_term.get(@key, %{})
end

[name] = System.argv()
identity = "tmp/#{name}.iroh"
{:ok, key} = IrohBeam.Identity.load_or_create(identity)
{:ok, id} = IrohBeam.SecretKey.endpoint_id(key)

{:ok, _pid} =
  IrohBeam.Distribution.start(
    name: :"#{name}@#{id}",
    identity: {:file, identity},
    network: :n0,
    admission: {Demo, :allowed?}
  )

Node.set_cookie(:iroh_beam_demo)
IO.puts("node #{Node.self()}")
