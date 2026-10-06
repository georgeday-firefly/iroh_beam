defmodule IrohBeam.TestAdmission do
  @moduledoc """
  Runtime-changeable admission set for distribution tests: endpoint ID hex =>
  the node-name part that ID may use. Used as `admission: {__MODULE__, :allowed?}`.
  """

  @key {__MODULE__, :allowed}

  def allow(id, name), do: :persistent_term.put(@key, Map.put(allowed(), to_string(id), name))

  def allowed?(id, nil), do: Map.has_key?(allowed(), id)
  def allowed?(id, name), do: Map.get(allowed(), id) == name

  defp allowed, do: :persistent_term.get(@key, %{})
end
